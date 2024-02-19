from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from passlib.context import CryptContext
from jose import jwt

import psycopg





CONN_PARAMS = {
    "dbname": "tiketto",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}




app = FastAPI()




class AuthRequest( BaseModel ):
    username: str
    password: str

class AccessToken( BaseModel ):
    accessToken: str

PWD_CONTEXT = CryptContext( schemes = [ "bcrypt" ], deprecated = "auto" )

SECRET_KEY = "8fsnjkj92ndmznie89q0kshdwpqwomxbvkjfwpu830edndshd3dwdcjkli9873bd"
ALGORITHM = "HS256"

def handle_authenticate( cur, credentials ):
    cur.execute(
        f"""
        select * from users.user
        where username = '{ credentials.username }'
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid credentials"
        )

    user_data = cur.fetchone()

    if( not PWD_CONTEXT.verify(
        credentials.password,
        user_data[ 3 ] # USER PASSWORD
    )):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid credentials"
        )

    user_data = {
        "user_id": user_data[ 0 ],
        "user_type": user_data[ 1 ],
        "username": user_data[ 2 ]
    }

    return AccessToken(
        accessToken = jwt.encode( user_data, SECRET_KEY, algorithm = ALGORITHM )
    )

@app.post( "/authenticate" )
async def authenticate(
    credentials: AuthRequest
) -> AccessToken:
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            return handle_authenticate( cur, credentials )





class TicketInfo( BaseModel ):
    movie_name: str
    movie_date: str
    ticket_num: str
    ticket_state: str

def handle_tickets( cur, ticket_id ):
    cur.execute(
        f"""
        select ms.movie_name, e.event_date, t.ticket_num, ts.state_type
        from logistics.movie_shows ms, logistics.reservation r, logistics.event e, logistics.ticket t, logistics.ticket_state ts
        where ms.event_id = r.event_id
        and r.event_id = e.event_id
        and r.reserv_id = t.reserv_id
        and t.ticket_id = ts.ticket_id
        and t.ticket_id = {ticket_id}
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

    ticket_info = cur.fetchone()

    return TicketInfo(
        movie_name = ticket_info[0],
        movie_date = ticket_info[1].strftime("%d/%m/%Y %H:%M"),
        ticket_num = str(ticket_info[2]),
        ticket_state = ticket_info[3]
    )

@app.get( "/tickets/{ticket_id}" )
async def tickets( ticket_id: int ) -> TicketInfo:
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            return handle_tickets( cur, ticket_id )
        




def handle_admit_ticket( conn, cur, ticket_id ):
    cur.execute(
        f"""
        update logistics.ticket_state
        set state_type = 'INVALID'
        where ticket_id = {ticket_id}
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

    conn.commit()

@app.post( "/tickets/admit/{ticket_id}" )
async def tickets( ticket_id: int ):
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            handle_admit_ticket( conn, cur, ticket_id )