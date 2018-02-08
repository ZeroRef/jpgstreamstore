DROP TABLE IF EXISTS public.jpg_stream_store_log;

CREATE TABLE public.jpg_stream_store_log (
	event_id SERIAL PRIMARY KEY,
	event_body text NOT NULL,
	stream_name varchar(100) NULL,
    stream_version integer NOT NULL,
    CONSTRAINT pk_stream_and_version UNIQUE(stream_name, stream_version)
);