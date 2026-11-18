
create table endpoint(
    id varchar not null primary key,
    urlstring varchar not null,
    lanecount integer not null default(5),
    consecfailures integer not null default(0),
    learnedrate integer,
    registeredrate integer,
    active  boolean not null default(true),
    failmessage varchar
);
create table resource(
    id varchar not null primary key,
    merchantid varchar not null,
    resourcetype varchar not null,
    resourcestate varchar not null check (resourcestate in (
        -- payments (pay_)
        'authorized', 'captured', 'failed',
        'refund_pending', 'refunded',
        'disputed', 'dispute_won', 'dispute_lost',
        -- subscriptions (sub_)
        'active', 'cancelled',
        -- payouts (po_)
        'paid'
    ))

);
create  table event(
seq bigint generated always as identity,
id varchar not null  primary key,
eventtype varchar not null,
merchantid varchar not null,
resourceid varchar not null references resource,
message jsonb not null,
eventbornat timestamptz not null,
handled boolean not null default(false)
);

create table delivery(
id uuid not null primary key default gen_random_uuid(),
eventid varchar unique not null references event,
endpointid varchar not null references endpoint,
resourceid varchar not null references resource,
state varchar not null default 'queued' check (state in ('queued','delivering','successful','retry_wait','parked','cancelled')),
retryat timestamptz,
attemptcount integer not null default(0)
);

create table attempt(
id uuid not null  primary key default gen_random_uuid(),
deliveryid uuid not null references delivery,
attempttime timestamptz not null,
error varchar,
statuscode integer,
durationms integer
);




create table subscriptions(
    merchantid  varchar not null,
    eventtype varchar not null,
    endpointid varchar not null references endpoint,
    primary key (merchantid,eventtype,endpointid)
);


