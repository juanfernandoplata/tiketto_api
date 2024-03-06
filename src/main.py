from dotenv import load_dotenv
import os

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel
from typing import Annotated, List
from enum import Enum

import psycopg

from passlib.context import CryptContext
from jose import JWTError, jwt

load_dotenv( "./config/.env" )

CONN_URL = os.environ.get( "CONN_URL" )

SECRET_KEY = os.environ.get( "SECRET_KEY" )

PWD_CONTEXT = CryptContext( schemes = [ "bcrypt" ], deprecated = "auto" )

ALGORITHM = "HS256"



app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = [ "*" ],
    allow_methods = [ "*" ],
    allow_headers = [ "*" ]
)



class BusinessUser( BaseModel ):
    user_id: int
    user_type: str
    comp_id: int
    user_role: str

def decode_token( access_token: str ) -> BusinessUser:
    try:
        return BusinessUser( **jwt.decode( access_token, SECRET_KEY, algorithms = [ ALGORITHM ] ) )
    
    except JWTError:
        raise HTTPException(
            status_code = 401,
            detail = "Invalid access token"
        )



def handle_business_auth( cur, username, password ):
    cur.execute(
        f"""
        select u.user_id, u.user_type, bu.comp_id, bu.user_role, u.u_password
        from users.user u, users.business_user bu
        where u.user_id = bu.user_id
        and username = '{ username }'
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid credentials"
        )

    user_data = cur.fetchone()

    if( not PWD_CONTEXT.verify(
        password,
        user_data[ -1 ] # USER PASSWORD
    )):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid credentials"
        )

    user_data = {
        "user_id": user_data[ 0 ],
        "user_type": user_data[ 1 ],
        "comp_id": user_data[ 2 ],
        "user_role": user_data[ 3 ]
    }

    return jwt.encode( user_data, SECRET_KEY, algorithm = ALGORITHM )

@app.post( "/business/authenticate" )
def business_auth(
    username: str,
    password: str
) -> str:
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            return handle_business_auth( cur, username, password )



class EventInfo( BaseModel ):
    event_id: int
    event_date: str
    event_time: str
    event_caracts: dict

def handle_get_events_offering( cur, comp_id, venue_id, event_type ):
    cur.execute(
        f"""
        select te.*, e.event_date
        from logistics.{ event_type }_event te, logistics.event e, logistics.venue v, logistics.location l
        where te.event_id = e.event_id
        and e.venue_id = v.venue_id
        and v.loc_id = l.loc_id
        and e.comp_id = { comp_id }
        and e.venue_id = { venue_id }
        and current_timestamp at time zone 'UTC' < e.offering_ends - interval '1 minute' * l.utc_offset
        """
    )

    if( not cur.rowcount ):
        return { "events": [] }

    events = []

    for event in cur.fetchall():
        event_caracts = {}
        for i in range( 1, len( cur.description ) - 1 ):
            event_caracts[ cur.description[ i ][ 0 ] ] = event[ i ]

        events.append( EventInfo(
            event_id = event[ 0 ],
            event_date = event[ -1 ].strftime( "%d/%m/%Y" ),
            event_time = event[ -1 ].strftime( "%H:%M" ),
            event_caracts = event_caracts
        ))

    return events

@app.get( "/business/venues/{venue_id}/events/{event_type}/offering" )
def get_events_offering(
    venue_id: int,
    event_type: str,

    user: Annotated[ BusinessUser, Depends( decode_token ) ]
) -> List[ EventInfo ]:
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            return handle_get_events_offering( cur, user.comp_id, venue_id, event_type )



def handle_get_event_availability( cur, event_id ):
    cur.execute(
        f"""
        select e.capacity
        from logistics.event e
        where e.event_id = { event_id }
        """
    )

    capacity = cur.fetchone()[ 0 ]

    cur.execute(
        f"""
        select sum(r.num_tickets)
        from logistics.reservation r
        where r.event_id = { event_id }
        and r.reserv_state in ('CONFIRMED', 'PENDING_CONFIRM')
        """
    )

    reserved = cur.fetchone()[ 0 ]

    return ( capacity - reserved )

@app.get( "/business/events/{event_id}/availability" )
def get_event_availability(
    event_id: int,

    user: Annotated[ BusinessUser, Depends( decode_token ) ]
) -> int:
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            return handle_get_event_availability( cur, event_id )



# Aqui se hacen validaciones de disponibilidad del evento.
# Sin embargo, estas verificaciones deberian incorporarse
# a la base de datos para asegurar la integridad hasta mas
# bajo nivel.
def handle_reserve_event( cur, event_id, client_id, num_tickets ):
    cur.execute(
        f"""
        select e.capacity
        from logistics.event e
        where e.event_id = { event_id }
        """
    )

    capacity = cur.fetchone()[ 0 ]

    cur.execute(
        f"""
        select sum(r.num_tickets)
        from logistics.reservation r
        where r.event_id = { event_id }
        and r.reserv_state in ('CONFIRMED', 'PENDING_CONFIRM')
        """
    )

    reserved = cur.fetchone()[ 0 ]

    if( num_tickets > capacity - reserved ):
        raise HTTPException(
            status_code = 500,
            detail = "There are not enough available tickets"
        )
    
    cur.execute(
        f"""
        select count(*)
        from logistics.client c
        where c.client_id = '{ client_id }'
        """
    )

    count = cur.fetchone()[ 0 ]

    if( not count ):
        cur.execute(
            f"""
            insert into logistics.client(client_id)
            values('{ client_id }')
            """
        )
    
    cur.execute(
        f"""
        insert into logistics.reservation(client_id, event_id, num_tickets, reserv_state)
        values(
            '{ client_id }',
            { event_id },
            { num_tickets },
            'PENDING_CONFIRM'
        )
        returning reserv_id
        """
    )

    reserv_id = cur.fetchone()[ 0 ]

    return reserv_id

@app.post( "/business/reservations" )
def reserve_event(
    event_id: int,
    client_id: str,
    num_tickets: int,

    user: Annotated[ BusinessUser, Depends( decode_token ) ]
) -> int:
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            return handle_reserve_event( cur, event_id, client_id, num_tickets )



# class EventStateEnum( str, Enum ):
#     pending_confirm = "PENDING_CONFIRM"
#     never_confirmed = "NEVER_CONFIRMED"
#     confirmed = "CONFIRMED"
#     canceled = "CANCELED"

# class EventState( BaseModel ):
#     event_state: EventStateEnum

# def handle_change_reservation_state( cur, reserv_id, event_state ):
#     cur.execute(
#         f"""
#         update logistics.reservation
#         set reserv_state = '{ event_state.event_state.value }'
#         where reserv_id = { reserv_id }
#         """
#     )

#     if( not cur.rowcount ):
#         raise HTTPException(
#             status_code = 404,
#             detail = "Resource not found"
#         )

# @app.put( "/business/reservations/{reserv_id}/state" )
# def change_reservation_state(
#     reserv_id: int,
#     event_state: EventState,

#     user: Annotated[ User, Depends( decode_token ) ]
# ):
#     with psycopg.connect( CONN_URL ) as conn:
#         with conn.cursor() as cur:
#             return handle_change_reservation_state( cur, reserv_id, event_state )



def handle_reservation_no_confirm( cur, reserv_id ):
    cur.execute(
        f"""
        update logistics.reservation
        set reserv_state = 'NEVER_CONFIRMED'
        where reserv_id = { reserv_id }
        and reserv_state = 'PENDING_CONFIRM'
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

@app.post( "/business/reservations/{reserv_id}/no_confirm" )
def reservation_no_confirm(
    reserv_id: int,

    user: Annotated[ BusinessUser, Depends( decode_token ) ]
):
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            return handle_reservation_no_confirm( cur, reserv_id )



def handle_reservation_confirm( cur, reserv_id ):
    cur.execute(
        f"""
        update logistics.reservation
        set reserv_state = 'CONFIRMED'
        where reserv_id = { reserv_id }
        and reserv_state = 'PENDING_CONFIRM'
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

    # Validar que se ejecuto bien el query
    cur.execute(
        f"""call logistics.create_tickets({ reserv_id })"""
    )

@app.post( "/business/reservations/{reserv_id}/confirm" )
async def reservation_confirm(
    reserv_id: int,

    user: Annotated[ BusinessUser, Depends( decode_token ) ]
):
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            return handle_reservation_confirm( cur, reserv_id )



class TicketInfo( BaseModel ):
    movie_name: str
    movie_date: str
    movie_time: str
    ticket_num: str
    ticket_state: str

def handle_get_ticket( cur, ticket_id ):
    cur.execute(
        f"""
        select me.movie_name, e.event_date, t.ticket_num, t.state_type
        from logistics.movie_event me, logistics.event e, logistics.reservation r, logistics.ticket t
        where me.event_id = e.event_id
        and me.event_id = r.event_id
        and r.reserv_id = t.reserv_id
        and t.ticket_id = { ticket_id }
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

    ticket_info = cur.fetchone()

    return TicketInfo(
        movie_name = ticket_info[ 0 ],
        movie_date = ticket_info[ 1 ].strftime( "%d/%m/%Y" ),
        movie_time = ticket_info[ 1 ].strftime( "%H:%M" ),
        ticket_num = "#" + str( ticket_info[ 2 ] ),
        ticket_state = ticket_info[ 3 ]
    )

@app.get( "/tickets/{ticket_id}" )
def get_ticket( ticket_id: int ) -> TicketInfo:
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            return handle_get_ticket( cur, ticket_id )



def handle_admit_ticket( cur, ticket_id ):
    cur.execute(
        f"""
        update logistics.ticket
        set state_type = 'INVALID'
        where ticket_id = { ticket_id }
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

@app.post( "/tickets/admit/{ticket_id}" )
def admit_ticket( ticket_id: int ):
    with psycopg.connect( CONN_URL ) as conn:
        with conn.cursor() as cur:
            handle_admit_ticket( cur, ticket_id )