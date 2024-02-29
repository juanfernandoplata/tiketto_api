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

DB_URL = "postgres://tiketto:1kWsULsQdTMhfof19OFRUjfDqzq2oY4Q@dpg-cnf90a5a73kc7391qfeg-a.oregon-postgres.render.com/tiketto"

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

@app.post( "/business/authenticate" )
async def businessAuth(
    credentials: AuthRequest
) -> AccessToken:
    with psycopg.connect( DB_URL ) as conn:
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
        select te.*, e.event_date, l.utc_offset
        from logistics.{ eventType }_event te, logistics.event e, logistics.venue v, logistics.location l
        where te.event_id = e.event_id
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
    with psycopg.connect( DB_URL ) as conn:
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

    # cur.execute(
    #     f"""
    #     select count(*)
    #     from logistics.event e, logistics.reservation r, logistics.ticket t
    #     where e.event_id = r.event_id
    #     and r.reserv_id = t.reserv_id
    #     and e.event_id = { eventId }
    #     and r.reserv_state != 'CANCELED'
    #     """
    # )

    cur.execute(
        f"""
        select sum(r.num_tickets)
        from logistics.reservation r
        where r.event_id = { eventId }
        and r.reserv_state != 'CANCELED'
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
    with psycopg.connect( DB_URL ) as conn:
        with conn.cursor() as cur:
            return handleEventAvailability( cur, eventId )





class ReservInfo( BaseModel ):
    eventId: int
    clientId: str
    numTickets: int

class ReservId( BaseModel ):
    reservId: int

# Aqui se hacen validaciones de disponibilidad del evento.
# Sin embargo, estas verificaciones deberian incorporarse
# a la base de datos para asegurar la integridad hasta mas
# bajo nivel.
def handleEventReserve( cur, reservInfo ):
    cur.execute(
        f"""
        select e.capacity
        from logistics.event e
        where e.event_id = { reservInfo.eventId }
        """
    )

    capacity = cur.fetchone()[ 0 ]

    cur.execute(
        f"""
        select sum(r.num_tickets)
        from logistics.reservation r
        where r.event_id = { reservInfo.eventId }
        and r.reserv_state != 'CANCELED'
        """
    )

    reserved = cur.fetchone()[ 0 ]

    if( reservInfo.numTickets > capacity - reserved ):
        raise HTTPException(
            status_code = 500,
            detail = "There are not enough available tickets"
        )
    
    cur.execute(
        f"""
        select count(*)
        from logistics.client c
        where c.client_id = '{ reservInfo.clientId }'
        """
    )

    count = cur.fetchone()[ 0 ]

    if( not count ):
        cur.execute(
            f"""
            insert into logistics.client(client_id)
            values('{ reservInfo.clientId }')
            """
        )
    
    cur.execute(
        f"""
        insert into logistics.reservation(client_id, event_id, num_tickets, reserv_state)
        values(
            '{ reservInfo.clientId }',
            { reservInfo.eventId },
            { reservInfo.numTickets },
            'PENDING'
        )
        returning reserv_id
        """
    )

    reservId = cur.fetchone()[ 0 ]

    return ReservId( reservId = reservId )

@app.post( "/business/events/{eventId}/reserve" )
async def eventReserve(
    reservInfo: ReservInfo,

    user: Annotated[ User, Depends( decodeToken ) ]
) -> ReservId:
    with psycopg.connect( DB_URL ) as conn:
        with conn.cursor() as cur:
            return handleEventReserve( cur, reservInfo )





def handleReservationCancel( cur, reservId ):
    cur.execute(
        f"""
        update logistics.reservation
        set reserv_state = 'CANCELED'
        where reserv_id = { reservId }
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

@app.post( "/business/reservations/{reservId}/cancel" )
async def reservationCancel(
    reservId: int,

    user: Annotated[ User, Depends( decodeToken ) ]
):
    with psycopg.connect( DB_URL ) as conn:
        with conn.cursor() as cur:
            return handleReservationCancel( cur, reservId )





def handleReservationConfirm( cur, reservId ):
    cur.execute(
        f"""
        update logistics.reservation
        set reserv_state = 'CONFIRMED'
        where reserv_id = { reservId }
        """
    )

    if( not cur.rowcount ):
        raise HTTPException(
            status_code = 404,
            detail = "Resource not found"
        )

    # Validar que se ejecuto bien el query
    cur.execute(
        f"""call create_tickets({ reservId })"""
    )

@app.post( "/business/reservations/{reservId}/confirm" )
async def reservationConfirm(
    reservId: int,

    user: Annotated[ User, Depends( decodeToken ) ]
):
    with psycopg.connect( DB_URL ) as conn:
        with conn.cursor() as cur:
            return handleReservationConfirm( cur, reservId )





class TicketInfo( BaseModel ):
    movieName: str
    movieDate: str
    ticketNum: str
    ticketState: str

def handleTickets( cur, ticketId ):
    cur.execute(
        f"""
        select me.movie_name, e.event_date, t.ticket_num, ts.state_type
        from logistics.movie_event me, logistics.reservation r, logistics.event e, logistics.ticket t, logistics.ticket_state ts
        where me.event_id = r.event_id
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
    with psycopg.connect( DB_URL ) as conn:
        with conn.cursor() as cur:
            return handleTickets( cur, ticketId )





def handleAdmitTicket( cur, ticketId ):
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

@app.post( "/tickets/admit/{ticketId}" )
async def tickets( ticketId: int ):
    with psycopg.connect( DB_URL ) as conn:
        with conn.cursor() as cur:
            handleAdmitTicket( cur, ticketId )