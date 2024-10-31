-- exec jobmanager
-- ./bin/sql-client.sh

CREATE TABLE Sessions (
                    id STRING,
                    `date` STRING,
                    speaker STRING,
                    title STRING
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'sessions',
                    'properties.bootstrap.servers' = 'broker:19092',
                    'properties.group.id' = 'flink-sessions-consumer',
                    'scan.startup.mode' = 'earliest-offset',
                    'value.format' = 'json'
                );


CREATE TABLE Registrations (
                    session_id STRING,
                    reg_ts STRING,
                    `ts` AS to_timestamp(reg_ts),
                    WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = 'registrations',
                    'properties.bootstrap.servers' = 'broker:19092',
                    'properties.group.id' = 'flink-registrations-consumer',
                    'scan.startup.mode' = 'earliest-offset',
                    'value.format' = 'json'
                );

CREATE TABLE SessionsPopularity (
                    session_id STRING,
                    `date` STRING,
                    title STRING,
                    speaker STRING,
                    registrations BIGINT,
                    PRIMARY KEY (session_id) NOT ENFORCED
                ) WITH (
                    'connector' = 'upsert-kafka',
                    'topic' = 'sessions-popularity',
                    'properties.bootstrap.servers' = 'broker:19092',
                    'properties.group.id' = 'flink-sessions-popularity-producer',
                    'key.format' = 'json',
                    'value.format' = 'json'
                );


SELECT COUNT(*) AS registrations, Registrations.session_id, Sessions.`date`, Sessions.title, Sessions.speaker
FROM Registrations INNER JOIN Sessions
ON Registrations.session_id = Sessions.id
GROUP BY Registrations.session_id, Sessions.`date`, Sessions.title, Sessions.speaker;

INSERT INTO SessionsPopularity SELECT Registrations.session_id, Sessions.`date`, Sessions.title, Sessions.speaker, COUNT(*) AS registrations
FROM Registrations INNER JOIN Sessions
ON Registrations.session_id = Sessions.id
GROUP BY Registrations.session_id, Sessions.`date`, Sessions.title, Sessions.speaker;
