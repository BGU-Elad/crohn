from src.utils.constants import START_LEVEL_DEFAULT

USER_LEVEL_QUERY = """
   SELECT userId as id, levelId as level
   FROM PositionLevel
"""

USER_TIME_QUERY = """
   SELECT id, morningPreferTime, eveningPreferTime
   FROM App_user
"""


LEVEL_OF_CURRENT_EXERCISE_QUERY = f"""
SELECT DISTINCT level_techniquesList.'שלב ' FROM Exercise JOIN actions ON Exercise.actionId = actions.'מספר הפעולה' JOIN
 level_techniquesList ON actions.'מספר טכניקה' = level_techniquesList.'טכניקה' WHERE Exercise.userId = {{user_id}} AND
 Exercise.dateStart > date(date('now', '-{{minus_time}} days'), '-{{days}} days')
"""

EXERCISES_OF_USER_QUERY = """
    WITH exersice_to_scores AS (
    SELECT e.userId, e.actionId, q1.suds_stress as sudsQ1, q2.suds_stress as sudsQ2, q1.fatigue AS fatigueQ1,
    q2.fatigue AS fatigueQ2, q1.vas_pain AS vasQ1, q2.vas_pain AS vasQ2 
    FROM Exercise as e, Questionnaire as q1, Questionnaire as q2
    WHERE e.questionnaireLastId != 0 
    AND e.questionnairePrimerId = q1.questionnaireId
    AND e.questionnaireLastId = q2.questionnaireId
    AND q1.userId = {user_id}
    AND strftime('%H:%M', e.dateStart)  {time_direction} '{user_hour}'
    )
    SELECT  a.'מספר הפעולה', a.'מספר טכניקה', json_group_array(e.sudsQ1), json_group_array(e.sudsQ2),
    json_group_array(e.fatigueQ1), json_group_array(e.fatigueQ2), json_group_array(e.vasQ1) , json_group_array(e.vasQ2)
    FROM exersice_to_scores AS e
    JOIN actions as a on e.actionId = a.'מספר הפעולה'
    WHERE e.userId = {user_id}
    GROUP BY e.actionId
    HAVING 
        AVG(CASE WHEN e.sudsQ2 <= e.sudsQ1 THEN 1.0 ELSE 0.0 END) >= {min_percent}
        AND AVG(CASE WHEN e.fatigueQ2 <= e.fatigueQ1 THEN 1.0 ELSE 0.0 END) >= {min_percent}
        AND AVG(CASE WHEN e.vasQ2 <= e.vasQ1 THEN 1.0 ELSE 0.0 END) >= {min_percent}
"""

EXERCISES_OF_METRIC_FOR_USER_QUERY = """
    WITH exersice_to_scores AS (
    SELECT e.userId, e.actionId, q1.suds_stress as sudsQ1, q2.suds_stress as sudsQ2, q1.fatigue AS fatigueQ1,
    q2.fatigue AS fatigueQ2, q1.vas_pain AS vasQ1, q2.vas_pain AS vasQ2 
    FROM Exercise as e, Questionnaire as q1, Questionnaire as q2
    WHERE e.questionnaireLastId != 0 
    AND e.questionnairePrimerId = q1.questionnaireId
    AND e.questionnaireLastId = q2.questionnaireId
    AND q1.userId = {user_id}
    )
    SELECT  a.'מספר הפעולה', a.'מספר טכניקה', json_group_array(e.{before}), json_group_array(e.{after})
    FROM exersice_to_scores AS e
    JOIN actions as a on e.actionId = a.'מספר הפעולה'
    WHERE e.userId = {user_id}
    GROUP BY e.actionId
    HAVING 
        AVG(e.sudsQ1 {suds_direction} e.sudsQ2) >= {suds_percent} AND 
        AVG(e.fatigueQ1 {fat_direction} e.fatigueQ2) >= {fatigue_percent} AND
        AVG(e.vasQ1 {vas_direction} e.vasQ2) >= {vas_percent}
"""


FORGOTTEN_EXERCISES_OF_USER_QUERY = f"""
     WITH 
     last_k_days AS (
     SELECT actionId FROM Exercise WHERE userId = {{user_id}}
     AND dateStart > date(date('now', '-{{minus_time}} days'), '-{{days}} days') 
     ),
     exersice_to_scores AS (
     SELECT e.userId, e.actionId, q1.suds_stress as sudsQ1, q2.suds_stress as sudsQ2, q1.fatigue AS fatigueQ1,
     q2.fatigue AS fatigueQ2, q1.vas_pain AS vasQ1, q2.vas_pain AS vasQ2 
     FROM Exercise as e, Questionnaire as q1, Questionnaire as q2
     WHERE e.questionnaireLastId != 0 
     AND e.actionId NOT IN last_k_days
     AND e.questionnairePrimerId = q1.questionnaireId
     AND e.questionnaireLastId = q2.questionnaireId
     AND q1.userId = {{user_id}}
     )
     SELECT  e.actionId, COUNT(*)
     FROM exersice_to_scores AS e
     JOIN actions as a on e.actionId = a.'מספר הפעולה'
     WHERE e.userId = {{user_id}}
     GROUP BY e.actionId
     HAVING 
         AVG(e.sudsQ1 >= e.sudsQ2) >= {{min_percent}} AND 
         AVG(e.fatigueQ1 >= e.fatigueQ2) >= {{min_percent}} AND
         AVG(e.vasQ1 >= e.vasQ2) >= {{min_percent}}
"""


FOURTH_CARUSAL_EXERCISE_OF_USER_QUERY = f"""
WITH

cutoff AS (
    -- simulated "today" = now - {{minus_time}} days
    -- cutoff = simulated_today - 60 days
    SELECT date(date('now', '-{{minus_time}} days'), '-{{days}} days') AS cutoff_date
),
-- last exercise per (technique, action) for this user
action_last AS (
    SELECT
        ta.'מספר טכניקה' as techniqueId,
        ta.'מספר הפעולה' as actionId,
        MAX(date(e.dateStart)) AS last_date
    FROM actions AS ta
    LEFT JOIN Exercise AS e
           ON e.actionId = ta.'מספר הפעולה'
          AND e.userId  = {{user_id}}
    WHERE e.dateStart IS NOT NULL
    GROUP BY ta.'מספר הפעולה', ta.'מספר טכניקה'
),

-- count, per technique, how many actions were exercised in the last 60 days
tech_stats AS (
    SELECT
        al.techniqueId as techniqueId,
        COUNT(*) AS num_actions,
        SUM(
            CASE
                WHEN al.last_date >= (SELECT cutoff_date FROM cutoff)
                THEN 1
                ELSE 0
            END
        ) AS num_recent,
        SUM(al.last_date IS NOT NULL) AS num_ever_exercised
    FROM action_last AS al
    GROUP BY al.techniqueId
), 
not_exercised AS (
    SELECT ts.techniqueId AS technique
    FROM techniques AS t
    JOIN tech_stats AS ts
          ON ts.techniqueId = t.'מספר טכניקה'
    WHERE ts.num_recent = 0 and ts.num_actions > 0
),


    tutorial_actions AS (
        SELECT a.'מספר הפעולה' AS action, a.'מספר טכניקה' AS technique FROM actions AS a WHERE a.'סוג פעולה' = 'G'
    ),
    
exersice_to_scores AS (
    SELECT *
    FROM (
        SELECT
            e.userId,
            e.actionId,
            q1.suds_stress AS sudsQ1,
            q2.suds_stress AS sudsQ2,
            q1.fatigue     AS fatigueQ1,
            q2.fatigue     AS fatigueQ2,
            q1.vas_pain    AS vasQ1,
            q2.vas_pain    AS vasQ2,
            ROW_NUMBER() OVER (
                PARTITION BY e.actionId
                ORDER BY e.dateStart DESC      -- last exercises
            ) AS rn
        FROM Exercise AS e
        JOIN Questionnaire AS q1
            ON e.questionnairePrimerId = q1.questionnaireId
        JOIN Questionnaire AS q2
            ON e.questionnaireLastId = q2.questionnaireId
        WHERE e.questionnaireLastId != 0
          AND q1.userId = {{user_id}}
    )
    WHERE rn <= {{examples}}            -- {{examples}} last EXERCISES per action
),

deteriorations AS (
    SELECT
        e.actionId,
        t.'מספר טכניקה' AS technique
    FROM exersice_to_scores AS e
    JOIN actions AS t
        ON e.actionId = t.'מספר הפעולה'
    WHERE e.userId = {{user_id}}
    GROUP BY e.actionId
    HAVING
        AVG(
            CASE
                WHEN e.sudsQ1 = e.sudsQ2
                 AND e.fatigueQ1 = e.fatigueQ2
                 AND e.vasQ1 = e.vasQ2
                 AND e.vasQ1 <= 3 -- largest value in SC0
                 AND e.fatigueQ1 <= 3 -- largest value in SC0
                 AND e.vasQ1 <= 3 -- largest value in SC0
                THEN 1
                ELSE (e.sudsQ1 > e.sudsQ2)
            END
        ) < {{deterior}}
    AND
        AVG(
            CASE
                WHEN e.sudsQ1 = e.sudsQ2
                 AND e.fatigueQ1 = e.fatigueQ2
                 AND e.vasQ1 = e.vasQ2
                 AND e.vasQ1 <= 3 -- largest value in SC0
                 AND e.fatigueQ1 <= 3 -- largest value in SC0
                 AND e.vasQ1 <= 3 -- largest value in SC0
                THEN 1
                ELSE (e.fatigueQ1 > e.fatigueQ2)
            END
        ) < {{deterior}}
    AND
        AVG(
            CASE
                WHEN e.sudsQ1 = e.sudsQ2
                 AND e.fatigueQ1 = e.fatigueQ2
                 AND e.vasQ1 = e.vasQ2
                 AND e.vasQ1 <= 3 -- largest value in SC0
                 AND e.fatigueQ1 <= 3 -- largest value in SC0
                 AND e.vasQ1 <= 3 -- largest value in SC0
                THEN 1
                ELSE (e.vasQ1 > e.vasQ2)
            END
        ) < {{deterior}}
),
    combined AS (SELECT technique, 0 AS reason FROM deteriorations UNION SELECT technique, 1 AS reason FROM not_exercised)
    SELECT ta.action, c.* FROM combined AS c JOIN tutorial_actions AS ta ON c.technique = ta.technique
"""

ID_TO_MESSAGE_QUERY = """
    SELECT r.'Msg1 - Male', r.'Msg2 - Male', r.'Msg3 - Male', r.'Msg1 - Female', r.'Msg2 - Female', r.'Msg3 - Female'
    FROM Rules as r WHERE r.'new rule number'= {id}
    """


USER_SEX_QUERY = """
SELECT gender FROM App_user WHERE id = {user}
"""

SHOULD_BE_UNIT_QUERY = f"""
    WITH start_level AS (Select a.'מספר הפעולה' as action_num
    FROM actions AS a WHERE 1 = a.'מספר טכניקה' AND a.'סוג פעולה' = 'T' LIMIT 1)
    SELECT dateStart FROM Exercise JOIN start_level
    WHERE userId = {{user}} AND dateStart > '{START_LEVEL_DEFAULT}' AND actionId = start_level.action_num
"""

LEVEL_DAYS_QUERY = """
SELECT l."שלב מס'", l.'תקופת השלב בימים '  FROM levels as l order by l."שלב מס'"
"""


IS_REAL_USER_QUERY = """
    SELECT COUNT(*) FROM App_user as a JOIN Sheet1 as s ON s.acount = a.username WHERE a.id = {user}
"""

LAST_TIME_MESSAGE_QUERY = """
    SELECT date FROM History_bots WHERE userId = {user} AND EXISTS (
    SELECT 1
    FROM History_bots
    WHERE rule = {e}
  ) ORDER BY date DESC LIMIT 1
"""

LAST_MESSAGE_QUERY = """
    SELECT date FROM History_bots WHERE userId = {user} and rule IS NOT NULL ORDER BY date DESC LIMIT 1
"""


TRENDS_QUERY = """
    WITH 
    last_x_mesurements AS (SELECT * FROM Exercise WHERE userId = {user}
     ORDER BY dateStart DESC LIMIT {measurements})
    SELECT q1.suds_stress AS sudsQ1, q1.fatigue AS fatigueQ1, q1.vas_pain AS vasQ1, q2.suds_stress AS sudsQ2,
    q2.fatigue AS fatigueQ2, q2.vas_pain AS vasQ2
    FROM last_x_mesurements as l JOIN Questionnaire as q1 on l.questionnairePrimerId = q1.questionnaireId
    JOIN Questionnaire as q2 on l.questionnaireLastId = q2.questionnaireId
    ORDER BY l.dateStart
"""

GET_N_EXERCISES_IN_PAST_X_DAYS = f"""
    SELECT COUNT(actionId) FROM Exercise WHERE userId = {{user}}
    AND dateStart > date(date('now', '-{{minus_time}} days'), '-{{days}} days')
"""

GET_N_DIFFERENT_EXERCISES_PER_X_SAMPLES = """
    SELECT COUNT(distinct actionId) FROM Exercise WHERE userId = {user} ORDER BY dateStart DESC LIMIT {samples}
"""

N_SESSIONS_PER_X_DAYS_THAT_DO_NOT_HAVE_AN_AFTER_AND_DONE_SESSION = f"""
SELECT endSession Is NULL OR endSession = '00:00:00.000000' FROM Session WHERE userId = {{user}}
AND startSession > date(date('now', '-{{minus_time}} days'), '-{{days}} days')
"""

X_SESSIONS_BACK_WITH_Y_SESSIONS_WHERE_AFTER_IS_HIGHER_THAN_BEFORE_IN_Z_SCALES_QUERY = """
WITH last_x_sessions AS (
    SELECT * FROM Exercise WHERE userId = {user} ORDER BY dateStart DESC LIMIT {x}
)
    SELECT q1.suds_stress < q2.suds_stress as suds, q1.fatigue < q2.fatigue as fatigue, q1.vas_pain < q2.vas_pain as vas
    FROM Questionnaire as q1, Questionnaire as q2, last_x_sessions as l
    WHERE l.questionnairePrimerId = q1.questionnaireId AND l.questionnaireLastId = q2.questionnaireId;
"""

MIN_N_DAYS_FOR_UNIT_QUERY = """
    SELECT "תקופת השלב בימים " FROM levels WHERE "שלב מס'" = {unit}
"""

MIN_N_EXERCISES_FOR_UNIT_QUERY = """
WITH technique AS (SELECT distinct a.'מספר טכניקה' as technique FROM actions as a WHERE a.'מספר הפעולה' IN ({actions}))
SELECT SUM(tech.'מינמום פעולות ') FROM technique t join techniques as tech on t.technique = tech.'מספר טכניקה'
"""


NUMBER_OF_DAYS_FOR_UNIT_QUERY = """
    SELECT dateStart FROM Exercise WHERE userId = {user} AND actionId  IN ({actions}) ORDER BY dateStart DESC LIMIT 1
"""
START_OF_UNIT_QUERY = """
SELECT startLevel FROM PositionLevel WHERE userId = {user} AND levelId = {unit}
"""

LAST_EXERCISE_DATE_QUERY = """
        SELECT dateEnd FROM Exercise WHERE userId = {user} and dateEnd IS NOT NULL ORDER BY dateStart DESC LIMIT 1
    """


TIME_SINCE_START_UNIT_QUERY = """
    SELECT dateStart
    FROM Exercise JOIN actions ON Exercise.actionId = actions.'מספר הפעולה'
    JOIN level_techniquesList ON actions.'מספר טכניקה' = level_techniquesList.'טכניקה'
     WHERE userId = {user} AND level_techniquesList.'טכניקה' = {unit} ORDER BY dateStart ASC LIMIT 1
    """

N_EXERCISES_QUERY = """
    SELECT count(*) FROM Exercise WHERE userId = {user} AND actionId  IN ({actions})
"""

EXERCISE_OF_UNIT_EXERCISES_QUERY = """
SELECT a.'מספר הפעולה', a.'מספר סידורי' FROM actions as a 
"""

CURRENT_UNIT_QUERY = """
    SELECT levelId FROM PositionLevel WHERE userId = {user}
"""

CURRENT_UNIT_TIME_QUERY = """
    SELECT startLevel FROM PositionLevel WHERE userId = {user}
"""
CURRENT_T_QUERY = """
    SELECT T1, T2, T3, T4, T5 FROM Sheet1 as s JOIN App_user as a on a.username = s.acount WHERE a.id = {user}

"""
