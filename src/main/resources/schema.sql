DROP TABLE IF EXISTS public.tbl_es_event_store;

CREATE TABLE public.tbl_es_event_store (
	event_id SERIAL PRIMARY KEY,
	event_body jsonb NOT NULL,
	event_type varchar(250) NOT NULL,
	stream_name varchar(100) NULL,
    stream_version integer NOT NULL,
    CONSTRAINT pk_mt_events_stream_and_version UNIQUE(stream_name, stream_version)
);