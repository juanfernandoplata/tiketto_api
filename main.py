from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel

from typing import Annotated, List

from passlib.context import CryptContext
from jose import JWTError, jwt

import psycopg

from datetime import datetime, timedelta

####################################################################################
# PARAMETRIZAR Y LLEVAR A .ENV

CONN_PARAMS = {
    "dbname": "tiketto",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

PWD_CONTEXT = CryptContext( schemes = [ "bcrypt" ], deprecated = "auto" )

SECRET_KEY = "8fsnjkj92ndmznie89q0kshdwpqwomxbvkjfwpu830edndshd3dwdcjkli9873bd"
ALGORITHM = "HS256"

# PARAMETRIZAR Y LLEVAR A .ENV
####################################################################################

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = [ "*" ],
    allow_methods = [ "*" ],
    allow_headers = [ "*" ]
)





class User( BaseModel ):
    userId: int
    userType: str
    compId: int
    userRole: str

def decodeToken( accessToken: str ) -> User:
    try:
        return User( **jwt.decode( accessToken, SECRET_KEY, algorithms = [ ALGORITHM ] ) )
    except JWTError:
        raise HTTPException(
            status_code = 401,
            detail = "Invalid access token"
        )





class AuthRequest( BaseModel ):
    username: str
    password: str

class AccessToken( BaseModel ):
    accessToken: str

def handleBusinessAuth( cur, credentials ):
    cur.execute(
        f"""
        select u.user_id, u.user_type, bu.comp_id, bu.user_role, u.u_password
        from users.user u, users.business_user bu
        where u.user_id = bu.user_id
        and username = '{ credentials.username }'
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid credentials"
        )

    userData = cur.fetchone()

    if( not PWD_CONTEXT.verify(
        credentials.password,
        userData[ -1 ] # USER PASSWORD
    )):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid credentials"
        )

    userData = {
        "userId": userData[ 0 ],
        "userType": userData[ 1 ],
        "compId": userData[ 2 ],
        "userRole": userData[ 3 ]
    }

    return AccessToken(
        accessToken = jwt.encode( userData, SECRET_KEY, algorithm = ALGORITHM )
    )

@app.post( "/business/authentication" )
async def businessAuth(
    credentials: AuthRequest
) -> AccessToken:
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            return handleBusinessAuth( cur, credentials )





class EventInfo( BaseModel ):
    eventId: int
    eventDate: str
    eventTime: str
    eventCaracts: dict

class EventInfoList( BaseModel ):
    events: List[ EventInfo ]

def handleEventsOffering( cur, compId, venueId, eventType ):
    cur.execute(
        f"""
        select s.*, e.event_date, l.utc_offset
        from logistics.{ eventType }_shows s, logistics.event e, logistics.venue v, logistics.location l
        where s.event_id = e.event_id
        and e.venue_id = v.venue_id
        and v.loc_id = l.loc_id
        and e.comp_id = { compId }
        and e.venue_id = { venueId }
        """
    )

    if( not cur.rowcount ):
        return { "events": [] }
    
    utcNow = datetime.utcnow()
    events = []

    for event in cur.fetchall():
        print( event )
        if( event[ -2 ] > utcNow + timedelta( minutes = event[ -1 ] ) ):
            eventCaracts = {}
            for i in range( 1, len( cur.description ) - 2 ):
                eventCaracts[ cur.description[ i ][ 0 ] ] = event[ i ]

            events.append( EventInfo(
                eventId = event[ 0 ],
                eventDate = event[ -2 ].strftime( "%d/%m/%Y" ),
                eventTime = event[ -2 ].strftime( "%H:%M" ),
                eventCaracts = eventCaracts
            ))

    return EventInfoList(
        events = events
    )

@app.get( "/business/venues/{venueId}/events/{eventType}/offering" )
async def eventsOffering(
    venueId: int,
    eventType: str,

    user: Annotated[ User, Depends( decodeToken ) ]
) -> EventInfoList:
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            return handleEventsOffering( cur, user.compId, venueId, eventType )





class Availability( BaseModel ):
    availability: int

def handleEventAvailability( cur, eventId ):
    cur.execute(
        f"""
        select e.capacity
        from logistics.event e
        where e.event_id = { eventId }
        """
    )

    capacity = cur.fetchone()[ 0 ]

    cur.execute(
        f"""
        select count(*)
        from logistics.event e, logistics.reservation r, logistics.ticket t
        where e.event_id = r.event_id
        and r.reserv_id = t.reserv_id
        and e.event_id = { eventId }
        """
    )

    reserved = cur.fetchone()[ 0 ]

    return Availability(
        availability = ( capacity - reserved )
    )

@app.get( "/business/events/{eventId}/availability" )
async def eventAvailability(
    eventId: int,

    user: Annotated[ User, Depends( decodeToken ) ]
) -> Availability:
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            return handleEventAvailability( cur, eventId )





class TicketInfo( BaseModel ):
    movieName: str
    movieDate: str
    ticketNum: str
    ticketState: str

def handleTickets( cur, ticketId ):
    cur.execute(
        f"""
        select ms.movie_name, e.event_date, t.ticket_num, ts.state_type
        from logistics.movie_shows ms, logistics.reservation r, logistics.event e, logistics.ticket t, logistics.ticket_state ts
        where ms.event_id = r.event_id
        and r.event_id = e.event_id
        and r.reserv_id = t.reserv_id
        and t.ticket_id = ts.ticket_id
        and t.ticket_id = { ticketId }
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

    ticketInfo = cur.fetchone()

    return TicketInfo(
        movieName = ticketInfo[ 0 ],
        movieDate = ticketInfo[ 1 ].strftime( "%d/%m/%Y %H:%M" ),
        ticketNum = "#" + str( ticketInfo[ 2 ] ),
        ticketState = ticketInfo[ 3 ]
    )

@app.get( "/tickets/{ticketId}" )
async def tickets( ticketId: int ) -> TicketInfo:
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            return handleTickets( cur, ticketId )





def handleAdmitTicket( conn, cur, ticketId ):
    cur.execute(
        f"""
        update logistics.ticket_state
        set state_type = 'INVALID'
        where ticket_id = { ticketId }
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

    conn.commit()

@app.post( "/tickets/admit/{ticketId}" )
async def tickets( ticketId: int ):
    with psycopg.connect( **CONN_PARAMS ) as conn:
        with conn.cursor() as cur:
            handleAdmitTicket( conn, cur, ticketId )