        -- DROP TABLE "data_analysis_db"."da_schema"."t_frigga_weather_map";
        -- mapping Logic
        CREATE TABLE "da_schema"."t_frigga_weather_map" AS
            WITH weather_temp AS (
                SELECT 
                    B.measure_date_kst
                    , A.device_serial
                    , A.device_lat
                    , A.device_lon
                    , A.aws_station_id
                    , A.aws_station_name
                    , A.aws_lat
                    , A.aws_lon
                    , A.distance_km
                    , B.ws1
                    , B.ws10
                    , B.ta
                    , B.re
                    , B.hm
                FROM 
                    "data_analysis_db"."da_schema"."v_corning_stn_01" AS A -- Corning에 납품되는 frigga device
                JOIN
                    "data_analysis_db"."da_schema"."v_corning_with_aws_01" AS B ON A.aws_station_id = B.stn
                WHERE 1=1
            )

        SELECT 
            T.device_datetime
            , T.device_datetime_kor
            , T.device_serial
            , T.lat
            , T.lon
            , T.battery
            , (CASE WHEN T.temperature = 3276 THEN NULL 
               WHEN T.temperature = 3276.7 THEN NULL
               ELSE T.temperature END) AS temperature
            , (CASE WHEN T.humidity = 126 THEN NULL ELSE T.humidity END) AS humidity
            , (CASE WHEN T.shock = 127 THEN NULL ELSE T.shock END) AS shock
            , (CASE WHEN T.acc = 32767 THEN NULL ELSE T.acc END) AS acc_orig
            , (CASE WHEN ROUND(sqrt(POWER(COALESCE(accx, 0), 2) + 
              POWER(COALESCE(accy, 0), 2) + 
              POWER(COALESCE(accz, 0), 2)), 2) = 5675.41 THEN NULL 
              ELSE ROUND(sqrt(POWER(COALESCE(accx, 0), 2) + 
              POWER(COALESCE(accy, 0), 2) + 
              POWER(COALESCE(accz, 0), 2)), 2) END)
              AS acc
            , (CASE WHEN T.accx = 3276.7 THEN NULL ELSE T.accx END) AS accx
            , (CASE WHEN T.accy = 3276.7 THEN NULL ELSE T.accy END) AS accy
            , (CASE WHEN T.accz = 3276.7 THEN NULL ELSE T.accz END) AS accz
            , (CASE WHEN T.tiltx = 3276.7 THEN NULL ELSE T.tiltx END) AS tiltx
            , (CASE WHEN T.tilty = 3276.7 THEN NULL ELSE T.tilty END) AS tilty
            , (CASE WHEN T.tiltz = 3276.7 THEN NULL ELSE T.tiltz END) AS tiltz
            , S.measure_date_kst
            , S.aws_station_id
            , S.aws_station_name
            , S.aws_lat
            , S.aws_lon
            , S.distance_km
            , S.ws1
            , S.ws10
            , S.ta
            , S.hm
        FROM 
            "data_analysis_db"."da_schema"."v_corning_target_01" AS T
        JOIN 
            weather_temp AS S ON T.device_serial = S.device_serial
                            AND T.lat=S.device_lat
                            AND T.lon=S.device_lon
                            AND T.device_datetime_kor BETWEEN S.measure_date_kst AND DATEADD(hour, 1, S.measure_date_kst)
        WHERE 1=1
            ORDER BY device_serial ASC, device_datetime_kor ASC
        ;