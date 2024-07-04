use snow_db;

show stages;
list @ext_s3_stage_snowpipe;

create or replace table imdb_top250_shows_raw(
    title varchar(100),
    year varchar(10),
    total_episodes varchar(10),
    age varchar(10),
    rating number(4,2),
    vote_count varchar(10),
    category varchar(50)
);

select * from imdb_top250_shows_raw;

copy into imdb_top250_shows_raw
    from @ext_s3_stage_snowpipe
    files = ('/imdb_shows_4.csv')
    file_format = csv_format
    validation_mode = return_all_errors
    on_error = abort_statement;
    
create or replace pipe imdb_snowpipe
  auto_ingest = true
  as
    copy into imdb_top250_shows_raw
      FROM @ext_s3_stage_snowpipe
      file_format = csv_format;

-- copy the SQS ARN and set it in the event notification for S3 bucket
show pipes;

describe pipe imdb_snowpipe;

select system$pipe_status('imdb_snowpipe');
-- lastIngestedTimestamp:		Timestamp of the most recent file successfully loaded by Snowpipe into the destination table
-- lastReceivedMessageTimestamp:	Timestamp of the last message received from the queue.
-- lastForwardedMessageTimestamp:	Last “create object” event message with a matching path/prefix that was forwarded to the pipe
-- lastPulledFromChannelTimestamp:	When Snowpipe last pulled “create object” event notifications for the pipe

select * from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('minute',-40,current_timestamp()),
    pipe_name=>'imdb_snowpipe'
));

alter pipe imdb_pipe set pipe_execution_paused = true;

select * from imdb_top250_shows_raw;

select 
    substr(title, 1, position('.' in title)-1)::number(5) show_rank,
    trim(substr(title, position('.' in title)+1))::varchar(100) show_title,
    year show_years,
    left(total_episodes, position(' ' in total_episodes)-1)::number(5) show_episodes,
    age show_age,
    rating show_rating,
    case
        when endswith(trim(vote_count,'()'),'K') then trim(vote_count,'()K')::double * 1000
        when endswith(trim(vote_count,'()'),'M') then trim(vote_count,'()M')::double * 1000000
    end as show_votes,
    category show_category
from imdb_top250_shows_raw
order by show_rank;

create table imdb_top250_shows
as
select 
    substr(title, 1, position('.' in title)-1)::number(5) show_rank,
    trim(substr(title, position('.' in title)+1))::varchar(100) show_title,
    year show_years,
    left(total_episodes, position(' ' in total_episodes)-1)::number(5) show_episodes,
    age show_age,
    rating show_rating,
    case
        when endswith(trim(vote_count,'()'),'K') then trim(vote_count,'()K')::double * 1000
        when endswith(trim(vote_count,'()'),'M') then trim(vote_count,'()M')::double * 1000000
    end as show_votes,
    category show_category
from imdb_top250_shows_raw
order by show_rank;

select * from imdb_top250_shows;

select get_ddl('table', 'imdb_top250_shows');

-- here imdb is a prefix for file names to unload
-- data unloaded without headers since file format skips it
-- data may be unloaded in multiple files.
copy into @ext_s3_stage_csv/imdb_250
    from imdb_top250_shows
file_format = csv_format
header = true;