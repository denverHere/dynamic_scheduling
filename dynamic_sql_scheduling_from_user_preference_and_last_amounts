CREATE OR REPLACE VIEW BASIN1_5555_FILL_LIST AS
WITH TANKS
     AS (SELECT WELL.GROUP_TYPE, TANK.TANK_NAME, TANK.TANK_ID
           FROM TANKS TANK
                LEFT JOIN WELLS WELL
                   ON (WELL.PRIMARY_ID = TANK.PRIMARY_ID)
          WHERE     TANK.DISCONTINUED_DT IS NULL
                AND WELL.GROUP_NAME IN ('BASIN1', 'BASIN2')),
     TI
     AS (SELECT A1.*
           FROM (SELECT DISTINCT
                        TANK.DIVISION_ID,
                        TANK.PRIMARY_ID,
                        ROW_NUMBER ()
                        OVER (PARTITION BY TANK.TANK_ID
                              ORDER BY VOL.RECORD_DT DESC)
                           AS RK,
                        TANK.TANK_NAME,
                        TANK.TANK_ID,
                        VOL.RECORD_DT AS LAST_RECORD_DT,
                        VOL.DAILY_VOLUME AS LAST_DAILY_VOLUME,
                        CHEM.CHEMICAL_NAME,
                        CHEM.CHEMICAL_TYPE AS LAST_CHEMICAL_TYPE,
                        CHEM.CHEMICAL_ID,
                        TANK.TARGET_RATE,
                        CASE
                           WHEN (  LAG (
                                      VOL.DAILY_VOLUME)
                                   OVER (PARTITION BY VOL.TANK_ID
                                         ORDER BY VOL.RECORD_DT ASC)
                                 + NVL (VOL.ADDED_VOLUME, 0)
                                 - VOL.DAILY_VOLUME) > -0.01
                           THEN
                              ROUND (
                                 (  (  LAG (
                                          VOL.DAILY_VOLUME)
                                       OVER (PARTITION BY VOL.TANK_ID
                                             ORDER BY VOL.RECORD_DT ASC)
                                     + NVL (VOL.ADDED_VOLUME, 0)
                                     - VOL.DAILY_VOLUME)
                                  / CASE
                                       WHEN (  VOL.RECORD_DT
                                             - LAG (
                                                  VOL.RECORD_DT)
                                               OVER (
                                                  PARTITION BY VOL.TANK_ID
                                                  ORDER BY VOL.RECORD_DT ASC)) =
                                               0
                                       THEN
                                          1
                                       ELSE
                                          (  VOL.RECORD_DT
                                           - LAG (
                                                VOL.RECORD_DT)
                                             OVER (
                                                PARTITION BY VOL.TANK_ID
                                                ORDER BY VOL.RECORD_DT ASC))
                                    END),
                                 2)
                           ELSE
                              NULL
                        END
                           LAST_INJ_RATE
                   FROM TANKS TANK
                        LEFT JOIN ICHEM_DBA.ICHEM_CHEMICAL CHEM
                           ON (CHEM.CHEMICAL_ID = TANK.CHEMICAL_ID)
                        LEFT JOIN (SELECT *
                                     FROM ICHEM_DBA.ICHEM_VOLUME VOL
                                    WHERE VOL.TANK_ID IS NOT NULL) VOL
                           ON (VOL.TANK_ID = TANK.TANK_ID)) A1
          WHERE A1.RK = 1),
     LAST_FILL
     AS (  SELECT TANK_ID, MAX (RECORD_DT) AS LAST_FILL_DT
             FROM ICHEM_DBA.ICHEM_VOLUME
            WHERE ADDED_VOLUME > 1
         GROUP BY TANK_ID)
  --
  --
  --
  --

  SELECT B1.TANK_NAME,
         CASE WHEN MAX(B1.VOL_TO_FILL) = 1 THEN 'REGULARLY SCHEDULED FILLS' ELSE MAX(B1.SPECIAL_INSTRUCTIONS) END SPECIAL_INSTRUCTIONS, 
         B1.CHEMICAL_NAME,
         MAX(B1.VOL_TO_FILL) AS VOL_TO_FILL,
         B1.RECORD_DT,
         B1.DAILY_VOLUME,
         B1.ADDED_VOLUME,
         B1.VENDOR_ACKNOWLEDGEMENT,
         B1.AS_FOUND,
         B1.AS_LEFT,
         B1.COMMENTS,
         B1.SOURCE_NAME,
         B1.DOES_NOT_NEED_FILL,
         B1.PERSON_RESPONSIBLE,
         B1.LAST_RECORD_DT,
         B1.TARGET_RATE,
         B1.LAST_INJ_RATE,
         B1.EST_EMPTY_DATE,
         B1.LAST_DAILY_VOLUME,
         B1.TANK_ID,
         B1.CHEMICAL_ID,
         B1.GROUP_SOURCE,
         MAX (B1.DATE_ASSIGNED) AS DATE_ASSIGNED,
         B1.TANK_NAME || MAX (B1.DATE_ASSIGNED) AS MAINKEY,
         B1.GROUP_TYPE
    FROM (SELECT TANKS.TANK_NAME,
                 NEXT_SAMPLE_DATE.SPECIAL_INSTRUCTIONS,
                 NEXT_SAMPLE_DATE.VOL_TO_FILL,
                 PERSON_RESPONSIBLE,
                 TI.LAST_RECORD_DT,
                 TI.LAST_DAILY_VOLUME,
                 TI.LAST_INJ_RATE,
                 CASE
                    WHEN     (   TI.TANK_NAME LIKE '%SWD%'
                              OR TI.TANK_NAME LIKE '%FW%'
                              OR TI.TARGET_RATE IS NULL)
                         AND TI.LAST_INJ_RATE > .01
                    THEN
                       TO_DATE (
                            TI.LAST_RECORD_DT
                          + ( (TI.LAST_DAILY_VOLUME) / TI.LAST_INJ_RATE))
                    WHEN TI.TARGET_RATE = 0
                    THEN
                       NULL
                    ELSE
                       TO_DATE (
                            TI.LAST_RECORD_DT
                          + ( (TI.LAST_DAILY_VOLUME) / TI.TARGET_RATE))
                 END
                    EST_EMPTY_DATE,
                 NULL AS RECORD_DT,
                 NULL AS DAILY_VOLUME,
                 NULL AS ADDED_VOLUME,
                 NULL AS_FOUND,
                 NULL AS_LEFT,
                 NULL AS VENDOR_ACKNOWLEDGEMENT,
                 NULL AS COMMENTS,
                 NULL AS SOURCE_NAME,
                 NEXT_SAMPLE_DATE.TANK_ID,
                 TI.TARGET_RATE,
                 TI.CHEMICAL_ID,
                 NULL AS GROUP_SOURCE,
                 TI.CHEMICAL_NAME,
                 NEXT_SAMPLE_DATE.NEXT_SAMPLE_DATE AS DATE_ASSIGNED,
                 NULL AS DOES_NOT_NEED_FILL,
                 TANKS.GROUP_TYPE
            FROM (                                   
                  SELECT TS.TANK_ID,
                         A.BATCH_DATE AS NEXT_SAMPLE_DATE,
                         NULL AS SPECIAL_INSTRUCTIONS,
                         1 AS VOL_TO_FILL,
                         PERSON_RESPONSIBLE
                    FROM (SELECT *
                            FROM ICHEM_DBA.TANK_CYCLE_SCHEDULE
                           WHERE     DISCONTINUED_DT IS NULL
                                 AND INSPECTION_TYPE = 1239) TS
                         LEFT JOIN
                         (WITH TEMP (CYCLE_NUMBER,
                                     N,
                                     BATCH_DATE,
                                     WEEKS_IN_CYCLE) 
                                          0 AS N,     
                                          BS.BATCH_DATE,
                                          CD.WEEKS_IN_CYCLE
                                     FROM (    SELECT 1 AS ONE,
                                                      TO_DATE (
                                                           NEXT_DAY (SYSDATE - 45,
                                                                     'MONDAY')
                                                         + ( (LEVEL - 1) * 7))
                                                         BATCH_DATE
                                                 FROM DUAL
                                           CONNECT BY LEVEL <= 1) BS
                                          LEFT JOIN
                                          (SELECT 1 AS ONE, C.*
                                             FROM ICHEM_DBA.BATCH_CYCLE_DETAILS C
                                            WHERE CYCLE_PURPOSE_ID = 81) CD
                                             ON (CD.ONE = BS.ONE)
                                   UNION ALL
                                   SELECT CYCLE_NUMBER,
                                          N + 1,
                                          (7 * WEEKS_IN_CYCLE) + BATCH_DATE,
                                          WEEKS_IN_CYCLE
                                     FROM TEMP
                                    WHERE     N < 500
                                          AND BATCH_DATE <
                                                 TO_DATE (SYSDATE + 120))
                          SELECT *
                            FROM TEMP
                           WHERE BATCH_DATE <= SYSDATE
                                                      ) A
                            ON (A.CYCLE_NUMBER = TS.CYCLE_NUMBER)
                  UNION
                  SELECT TANK_ID,
                         DATE_DUE,
                         SPECIAL_INSTRUCTIONS,
                         VOL_TO_FILL,
                         PERSON_RESPONSIBLE
                    FROM ICHEM_DBA.SPECIAL_REQUEST_TANK SRT
                   WHERE     SRT.INSPECTION_TYPE = 24
                         AND SRT.DISCONTINUED_DT IS NULL
                         AND TO_DATE (DATE_DUE) <= TO_DATE (SYSDATE)
                         AND TO_DATE (DATE_DUE) >= TO_DATE (SYSDATE - 120)                                                                         )
                 NEXT_SAMPLE_DATE
                 LEFT JOIN TANKS ON (TANKS.TANK_ID = NEXT_SAMPLE_DATE.TANK_ID)
                 LEFT JOIN TI ON (TI.TANK_ID = NEXT_SAMPLE_DATE.TANK_ID)
                 LEFT JOIN LAST_FILL
                    ON (LAST_FILL.TANK_ID = NEXT_SAMPLE_DATE.TANK_ID)
                 LEFT JOIN (SELECT TANK_ID, DATE_DUE
                              FROM ICHEM_DBA.SPECIAL_REQUEST_TANK
                             WHERE INSPECTION_TYPE = 1243) SRTR
                    ON (SRTR.TANK_ID = NEXT_SAMPLE_DATE.TANK_ID)
           WHERE     NEXT_SAMPLE_DATE.NEXT_SAMPLE_DATE > LAST_FILL.LAST_FILL_DT
                 AND (   NEXT_SAMPLE_DATE.NEXT_SAMPLE_DATE > SRTR.DATE_DUE
                      OR SRTR.DATE_DUE IS NULL)                    
                      ) B1
GROUP BY B1.TANK_NAME,
         B1.CHEMICAL_NAME,
         B1.RECORD_DT,
         B1.DAILY_VOLUME,
         B1.ADDED_VOLUME,
         B1.VENDOR_ACKNOWLEDGEMENT,
         B1.AS_FOUND,
         B1.AS_LEFT,
         B1.COMMENTS,
         B1.SOURCE_NAME,
         B1.DOES_NOT_NEED_FILL,
         B1.PERSON_RESPONSIBLE,
         B1.LAST_RECORD_DT,
         B1.TARGET_RATE,
         B1.LAST_INJ_RATE,
         B1.EST_EMPTY_DATE,
         B1.LAST_DAILY_VOLUME,
         B1.TANK_ID,
         B1.CHEMICAL_ID,
         B1.GROUP_SOURCE,
         B1.GROUP_TYPE
