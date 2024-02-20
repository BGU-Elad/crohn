
EXERCISES_IN_PROGRESS_QUERY = """
        SELECT json_group_array(a.'מספר סידורי') as actions, json_group_array(s.endSession) as sessions
        FROM Session as s JOIN list_exercises_ids as l on s.sessionId = l.sessionId
        JOIN Exercise as e on l.list_exercises_ids = e.exerciseId
        JOIN actions as a on e.actionId = a.'מספר הפעולה'
        WHERE s.userId = {userid} AND s.startSession >= '{date}'
        """

USER_LEVEL_QUERY = """
   SELECT userId as id, levelId as level
   FROM PositionLevel
"""

NON_IMPROVED_QUERY = """
            SELECT submain.userID as id, count(DISTINCT submain.sessionId) as count, sub.endSession as endSession, json_group_array(submain.endSession) as sessions, json_group_array(submain.typeSession) as session_type, json_group_array(DISTINCT excercise.actionId) as actions
            From
                Session AS submain JOIN
                (
                SELECT userId,MAX(endSession) AS endSession
                FROM Session
                WHERE (well_beingQ2 > well_beingQ1 AND ( sudsQ1 > sudsQ2 OR fatigueQ1 > fatigueQ2 OR vasQ1 > vasQ2))
                GROUP BY userId
                ) AS sub
            on submain.userId = sub.userId JOIN
            list_exercises_ids AS list ON submain.sessionId = list.sessionId JOIN
            Exercise AS excercise ON list.list_exercises_ids = excercise.exerciseId 
            where submain.endSession > sub.endSession
            GROUP BY submain.userId
        """

USER_WHERE = "WHERE userId = {user_id}"

CURRENT_PROGRESS_QUERY = "SELECT userId, json_group_array(levelId) FROM PositionLevel group by userId"

USER_TO_FINISHED_EXERCISES = """
            SELECT af.userId, json_group_array(af.techniqueId), json_group_array(a.'מספר סידורי'), json_group_array(a.'מספר הפעולה')
            FROM ActionFinish AS af JOIN actions AS a on af.actionId == a.'מספר הפעולה'
            {where}
            GROUP BY af.userId
        """

ACTIONS_NOT_DONE_QUERY = """
    SELECT *
    FROM actions as a
    WHERE 
    a.'מספר הפעולה' NOT IN ({already_done})
"""

AVAILABLE_ACTIONS_QUERY = """
                SELECT *
                FROM actions as a
                WHERE 
                a.'מספר הפעולה' NOT IN ({action}) 
                AND a.'מספר טכניקה' IN ({technique})
            """

TRAINING_DATA_QUERY = """
           SELECT s.userId, a.'מספר סידורי',  s.sessionId, s.sudsQ1, s.sudsQ2, s.fatigueQ1, s.fatigueQ2, s.vasQ1, s.vasQ2, s.well_beingQ1, s.well_beingQ2
           FROM Session as s JOIN list_exercises_ids as l on s.sessionId = l.sessionId
           JOIN Exercise as e on l.list_exercises_ids = e.exerciseId
           JOIN actions as a on e.actionId = a.'מספר הפעולה'
           WHERE CAST(a.'מספר סידורי' AS REAL) < {free_user_level} {where}
           GROUP BY s.userId, a.'מספר סידורי', s.sessionId
        """

