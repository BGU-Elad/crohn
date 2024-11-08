from datetime import datetime, timedelta
import json
from typing import Optional
import random
import pandas as pd

from src.heuristics.queries import EXERCISES_IN_PROGRESS_QUERY, USER_LEVEL_QUERY, NON_IMPROVED_QUERY, USER_WHERE, \
    CURRENT_PROGRESS_QUERY, USER_TO_FINISHED_EXERCISES, ACTIONS_NOT_DONE_QUERY, AVAILABLE_ACTIONS_QUERY, \
    TRAINING_DATA_QUERY, USER_TIME_QUERY
from src.utils.session_object import Session
import numpy as np
from scipy.stats import linregress


FREE_USER_LEVEL = 5
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
NOW= "now" #"2023-07-13 14:39:58" #
ESCILATION_FEATURE_DATE = "2024-07-15 14:39:58"
START_LEVEL_DEFAULT = '2023-07-20 17:25:15'
DAYS = 14
MORNING = 0
EVENING = 1
SUDS = 0
FATIGUE = 1
VAS = 2
INC = 0
STAG = 1
DET = 2
MISC = 3
WITHIN = 0
BETWEEN = 1

class HumanExpert:
    def __init__(self, conn):
        self.conn = conn


    def get_users(self):
        query = USER_LEVEL_QUERY
        self.conn.cur.execute(query)
        users = self.conn.cur.fetchall()
        return users

    def score_to_CS(self, score):
        if score < 4:
            return 1
        if score < 8:
            return 2
        return 3

    def score_pair_to_CS(self, score1, score2):
        score1 = self.score_to_CS(score1)
        score2 = self.score_to_CS(score2)
        if score1 == 3 and score2 == 1:
            return 1
        if score1 == 3 and score2 == 2:
            return 2
        if score1 == 2 and score2 == 1:
            return 3
        if score1 == 3 and score2 == 3:
            return 4
        if score1 == 2 and score2 == 2:
            return 5
        if score1 == 1 and score2 == 1:
            return 6
        return -1

    def get_users_time(self):
        time_format = "%H:%M"

        query = USER_TIME_QUERY
        self.conn.cur.execute(query)
        users = self.conn.cur.fetchall()
        user_time_map = {}
        for user in users:
            user = list(user)
            if user[1] == None:
                user[1] = "08:00"
            if user[2] == None:
                user[2] = "20:00"
            morn = datetime.strptime(user[1], time_format)
            even = datetime.strptime(user[2], time_format)
            user_time_map[user[0]] = ((morn + (even - morn)/2).time()).strftime(time_format)
        return user_time_map

    def technique_order(self, technique):
        if technique == 1:
            return 1
        if technique == [2,3]:
            return 2
        return 3

    def get_level_of_current_exercise(self, user_id: int, days: int=14):
        query = f"""
            SELECT DISTINCT level_techniquesList.'שלב ' FROM Exercise JOIN actions ON Exercise.actionId = actions.'מספר הפעולה' JOIN level_techniquesList ON actions.'מספר טכניקה' = level_techniquesList.'טכניקה' WHERE Exercise.userId = {user_id} AND Exercise.dateStart > date('now', '-{days} days') 
        """
        self.conn.cur.execute(query)
        levels = self.conn.cur.fetchall()
        return [l[0] for l in levels]

    def get_exercise_of_user(self, user_id: int, time: int, user_hour:str, min_percent: float = 0.8):
        levels = self.get_level_of_current_exercise(user_id)
        time_direction = "<" if time ==0 else ">"
        query = f"""
            WITH exersice_to_scores AS (
            SELECT e.userId, e.actionId, q1.suds_stress as sudsQ1, q2.suds_stress as sudsQ2, q1.fatigue AS fatigueQ1, q2.fatigue AS fatigueQ2, q1.vas_pain AS vasQ1, q2.vas_pain AS vasQ2 
            FROM Exercise as e, Questionnaire as q1, Questionnaire as q2
            WHERE e.questionnaireLastId != 0 
            AND e.questionnairePrimerId = q1.questionnaireId
            AND e.questionnaireLastId = q2.questionnaireId
            AND q1.userId = {user_id}
            AND strftime('%H:%M', e.dateStart)  {time_direction} '{user_hour}'
            )
            SELECT  a.'מספר הפעולה', a.'מספר טכניקה', json_group_array(e.sudsQ1), json_group_array(e.sudsQ2), json_group_array(e.fatigueQ1), json_group_array(e.fatigueQ2), json_group_array(e.vasQ1) , json_group_array(e.vasQ2)
            FROM exersice_to_scores AS e
            JOIN actions as a on e.actionId = a.'מספר הפעולה'
            WHERE e.userId = {user_id}
            GROUP BY e.actionId
            HAVING 
                AVG(e.sudsQ1 > e.sudsQ2) >= {min_percent} AND 
                AVG(e.fatigueQ1 > e.fatigueQ2) >= {min_percent} AND
                AVG(e.vasQ1 > e.vasQ2) >= {min_percent}
        """

        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        exercises = [e for e in exercises]
        exercises_dict = {}
        for exercise in exercises:
            technique = self.technique_order(exercise[1])
            suds1 = json.loads(exercise[2])
            suds2 = json.loads(exercise[3])
            fatigue1 = json.loads(exercise[4])
            fatigue2 = json.loads(exercise[5])
            vas1 = json.loads(exercise[6])
            vas2 = json.loads(exercise[7])

            suds_scores = [self.score_pair_to_CS(suds1[i], suds2[i]) for i in range(len(suds1))]
            relative_deltas_suds = [(suds1[i] - suds2[i])/suds1[i] for i in range(len(suds1))]
            relative_deltas_suds = np.array(relative_deltas_suds)
            suds_max_index = np.argmax(suds_scores)
            max_suds = suds_scores[suds_max_index]

            fat_scores = [self.score_pair_to_CS(fatigue1[i], fatigue2[i]) for i in range(len(fatigue1))]
            relative_deltas_fat = [(fatigue1[i] - fatigue2[i])/fatigue1[i] for i in range(len(fatigue1))]
            relative_deltas_fat = np.array(relative_deltas_fat)
            fat_max_index = np.argmax(fat_scores)
            max_fatigue = fat_scores[fat_max_index]

            vas_scores = [self.score_pair_to_CS(vas1[i], vas2[i]) for i in range(len(vas1))]
            relative_deltas_vas = [(vas1[i] - vas2[i])/vas1[i] for i in range(len(vas1))]
            relative_deltas_vas = np.array(relative_deltas_vas)
            vas_max_index = np.argmax(vas_scores)
            max_vas = vas_scores[vas_max_index]

            metrics = [max_suds, max_fatigue, max_vas]
            metrics_arg_max = [suds_max_index, fat_max_index, vas_max_index]
            relative_deltas = [relative_deltas_suds, relative_deltas_fat, relative_deltas_vas]

            max_change_index = np.argmax(metrics)
            max_change = metrics[max_change_index]

            relative_delta = relative_deltas[max_change_index][metrics_arg_max[max_change_index]]
            relative_delta_count = len(relative_deltas[max_change_index][ relative_deltas[max_change_index] == relative_delta])
            exercises_dict[exercise[0]] = {
                "exercise": exercise[0],
                "technique": self.technique_order(exercise[1]),
                "change": max_change,
                "relative_delta": relative_delta,
                "count": relative_delta_count,
                "in_out_unit": exercise[0] in levels,
            }
        return exercises_dict

    def take_two_from_each_cycle(self, lists):
        result = []
        while any(lists):
            for lst in lists:
                if len(lst) >= 2:
                    result.extend(lst[:2])
                    del lst[:2]
                elif len(lst) == 1:
                    result.append(lst[0])
                    del lst[0]
        return result

    def first_carusal(self, time: int):
        # get all users
        users = self.get_users()
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        user_time = self.get_users_time()
        if time == 0:
            time = MORNING
        else:
            time = EVENING
        exercise_for_user = {}
        for user in users:
            user_hour = user_time[user[0]]
            exercise_of_user = self.get_exercise_of_user(user_id=user[0], time=time, user_hour=user_hour)
            df = pd.DataFrame(exercise_of_user).T
            sorted_exercises = []
            if len(df) > 0:
                df["relative_delta"] = -1 * df["relative_delta"]
                df["count"] = -1 * df["count"]
                df = df.sort_values(["technique", "change", "relative_delta", "count", "in_out_unit"])
                df["relative_delta"] = -1 * df["relative_delta"]
                df["count"] = -1 * df["count"]
                groups = [d for d in df.groupby("technique")]
                new_groups = []
                for group in groups:
                    new_groups.append(group[1]["exercise"].tolist())
                sorted_exercises = self.take_two_from_each_cycle(new_groups)
            exercise_for_user[user[0]] = sorted_exercises
        return exercise_for_user



    def get_exercise_for_metric_of_user(self, user_id: int, metric: int, metric_percent:int=0.80, other_percent:int=0.40):
        levels = self.get_level_of_current_exercise(user_id)
        suds_percent = fatigue_percent = vas_percent = other_percent
        before = after = ""
        if metric == SUDS:
            suds_percent = metric_percent
            before = "sudsQ1"
            after = "sudsQ2"
        if metric == FATIGUE:
            fatigue_percent = metric_percent
            before = "fatigueQ1"
            after = "fatigueQ2"
        if metric == VAS:
            vas_percent = metric_percent
            before = "vasQ1"
            after = "vasQ2"

        query = f"""
            WITH exersice_to_scores AS (
            SELECT e.userId, e.actionId, q1.suds_stress as sudsQ1, q2.suds_stress as sudsQ2, q1.fatigue AS fatigueQ1, q2.fatigue AS fatigueQ2, q1.vas_pain AS vasQ1, q2.vas_pain AS vasQ2 
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
                AVG(e.sudsQ1 > e.sudsQ2) >= {suds_percent} AND 
                AVG(e.fatigueQ1 > e.fatigueQ2) >= {fatigue_percent} AND
                AVG(e.vasQ1 > e.vasQ2) >= {vas_percent}
        """

        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        exercises = [e for e in exercises]
        exercises_dict = {}
        for exercise in exercises:
            technique = self.technique_order(exercise[1])
            met1 = json.loads(exercise[2])
            met2 = json.loads(exercise[3])

            met_scores = [self.score_pair_to_CS(met1[i], met2[i]) for i in range(len(met1))]
            relative_deltas_met = [(met1[i] - met2[i])/(met1[i]+1) for i in range(len(met1))]
            relative_deltas_met = np.array(relative_deltas_met)
            met_max_index = np.argmax(met_scores)
            max_met = met_scores[met_max_index]

            relative_delta = relative_deltas_met[met_max_index]
            relative_delta_count = len(relative_deltas_met[ relative_deltas_met == relative_delta])
            exercises_dict[exercise[0]] = {
                "exercise": exercise[0],
                "change": max_met,
                "relative_delta": relative_delta,
                "count": relative_delta_count,
                "in_out_unit": exercise[0] in levels,
            }
        return exercises_dict

    def second_carusal(self, metric: int, metric_percent:int=0.80, other_percent:int=0.40):
        users = self.get_users()
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        exercise_for_user = {}
        for user in users:
            exercise_of_user = self.get_exercise_for_metric_of_user(user_id=user[0], metric=metric, metric_percent=metric_percent, other_percent=other_percent)
            df = pd.DataFrame(exercise_of_user).T
            sorted_exercises = []
            if len(df) > 0:
                df["relative_delta"] = -1 * df["relative_delta"]
                df["count"] = -1 * df["count"]
                df = df.sort_values(["change", "relative_delta", "count", "in_out_unit"])
                df["relative_delta"] = -1 * df["relative_delta"]
                df["count"] = -1 * df["count"]
                sorted_exercises = df["exercise"].tolist()
            exercise_for_user[user[0]] = sorted_exercises
        return exercise_for_user

    def get_forgoten_exercise_of_user(self, user_id: int, days:int=30, min_percent: float = 0.8):
        query = f"""
             WITH 
             last_k_days AS (
             SELECT actionId FROM Exercise WHERE userId = {user_id} AND dateStart > date('now', '-{days} days') 
             ),
             exersice_to_scores AS (
             SELECT e.userId, e.actionId, q1.suds_stress as sudsQ1, q2.suds_stress as sudsQ2, q1.fatigue AS fatigueQ1, q2.fatigue AS fatigueQ2, q1.vas_pain AS vasQ1, q2.vas_pain AS vasQ2 
             FROM Exercise as e, Questionnaire as q1, Questionnaire as q2
             WHERE e.questionnaireLastId != 0 
             AND e.actionId NOT IN last_k_days
             AND e.questionnairePrimerId = q1.questionnaireId
             AND e.questionnaireLastId = q2.questionnaireId
             AND q1.userId = {user_id}
             )
             SELECT  e.actionId, COUNT(*)
             FROM exersice_to_scores AS e
             JOIN actions as a on e.actionId = a.'מספר הפעולה'
             WHERE e.userId = {user_id}
             GROUP BY e.actionId
             HAVING 
                 AVG(e.sudsQ1 > e.sudsQ2) >= {min_percent} AND 
                 AVG(e.fatigueQ1 > e.fatigueQ2) >= {min_percent} AND
                 AVG(e.vasQ1 > e.vasQ2) >= {min_percent}
         """

        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        exercises = [e for e in exercises]
        exercises_dict = {}
        for exercise in exercises:
            exercises_dict[exercise[0]] = {
                "exercise": exercise[0],
                "count": exercise[1]
            }
        return exercises_dict

    def third_carusal(self, days:int=30, min_percent: float = 0.8):
        users = self.get_users()
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        exercise_for_user = {}
        for user in users:
            exercise_of_user = self.get_forgoten_exercise_of_user(user_id=user[0], days=days, min_percent=min_percent)
            df = pd.DataFrame(exercise_of_user).T
            sorted_exercises = []
            if len(df) > 0:
                df = df.sort_values(["count"])
                sorted_exercises = df["exercise"].tolist()
            exercise_for_user[user[0]] = sorted_exercises
        return exercise_for_user

    def get_fourth_carusal_exercise_of_user(self, user_id, days=60, deterior=0.5):
        query = f"""
            WITH
                tutorial_actions AS (
                    SELECT a.'מספר הפעולה' AS action, a.'מספר טכניקה' AS technique FROM actions AS a WHERE a.'סוג פעולה' = 'G'
                ),
                exercised_in_the_pas_k_days AS (
                    SELECT actionId FROM Exercise WHERE userId = {user_id} AND dateStart > date('now', '-{days} days') 
                ),
                not_exercised AS (
                    SELECT action, technique FROM actions JOIN tutorial_actions ON actions.'מספר הפעולה' = tutorial_actions.action
                    WHERE action NOT IN exercised_in_the_pas_k_days
                ),
                exersice_to_scores AS (
                    SELECT e.userId, e.actionId, q1.suds_stress as sudsQ1, q2.suds_stress as sudsQ2, q1.fatigue AS fatigueQ1, q2.fatigue AS fatigueQ2, q1.vas_pain AS vasQ1, q2.vas_pain AS vasQ2 
                    FROM Exercise as e, Questionnaire as q1, Questionnaire as q2
                    WHERE e.questionnaireLastId != 0 
                    AND e.questionnairePrimerId = q1.questionnaireId
                    AND e.questionnaireLastId = q2.questionnaireId
                    AND q1.userId = {user_id}
                ),
                deteriorations AS (
                    SELECT  e.actionId, t.technique
                     FROM exersice_to_scores AS e
                     JOIN actions as a on e.actionId = a.'מספר הפעולה'
                     JOIN tutorial_actions as t on a.'מספר הפעולה' = t.action
                     WHERE e.userId = {user_id}
                     GROUP BY e.actionId
                     HAVING 
                         AVG(e.sudsQ1 > e.sudsQ2) < {deterior} AND 
                         AVG(e.fatigueQ1 > e.fatigueQ2) < {deterior} AND
                         AVG(e.vasQ1 > e.vasQ2) < {deterior}
                ),
                combined AS (SELECT technique FROM deteriorations UNION SELECT technique FROM not_exercised)
                SELECT DISTINCT technique FROM combined
        """
        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        exercises = [e for e in exercises]
        exercises_dict = {}
        for exercise in exercises:
            exercises_dict[exercise[0]] = {
                "exercise": exercise[0],
            }
        return exercises_dict

    def fourth_carusal(self):
        users = self.get_users()
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        exercise_for_user = {}
        for user in users:
            exercise_of_user = self.get_fourth_carusal_exercise_of_user(user_id=user[0])
            df = pd.DataFrame(exercise_of_user).T
            sorted_exercises = []
            if len(df) > 0:
                df = df.sort_values(["exercise"])
                sorted_exercises = df["exercise"].tolist()
            exercise_for_user[user[0]] = sorted_exercises
        return exercise_for_user

    def recommend(self, time: Optional[int] = 0):
        assert time in [0,1], "time 0 is morning and time 1 is evening"
        first_carusal = self.first_carusal(time)
        second1_carusal = self.second_carusal(SUDS)
        second2_carusal = self.second_carusal(FATIGUE)
        second3_carusal = self.second_carusal(VAS)
        third_carusal = self.third_carusal()
        fourth_carusal = self.fourth_carusal()
        messages, message_indexes = self.get_messages()
        user_to_recommendation = {}
        all_users = set(first_carusal.keys()).union(second1_carusal.keys()).union(second2_carusal.keys()).union(second3_carusal.keys()).union(third_carusal.keys()).union(fourth_carusal.keys())
        for user in all_users:
            user_to_recommendation[user] = {
                "001A" if time == MORNING else "001B": first_carusal.get(user, []),
                "002A": second1_carusal.get(user, []),
                "002B": second2_carusal.get(user, []),
                "002C": second3_carusal.get(user, []),
                "003": third_carusal.get(user, []),
                "004": fourth_carusal.get(user, []),
                "message": messages.get(user, "")
            }
        return user_to_recommendation

    def id_to_message(self, id, m_or_f):
        query = f"""
        SELECT r.'Msg1 - Male', r.'Msg2 - Male', r.'Msg3 - Male', r.'Msg1 - Female', r.'Msg2 - Female', r.'Msg3 - Female' FROM Rules as r WHERE r.'new rule number'= {id}
        """
        self.conn.cur.execute(query)
        message = self.conn.cur.fetchone()
        if message is None:
            return ""
        if m_or_f == "m":
            message = message[0:3]
        else:
            message = message[3:]

        message = random.choice(message)
        return message


    def get_user_sex(self, user):
        query = f"""
        SELECT gender FROM App_user WHERE id = {user}
        """
        self.conn.cur.execute(query)
        sex = self.conn.cur.fetchone()
        return sex[0] if type(sex) == tuple else sex



    def get_should_be_unit(self, user):
        query = f"""
        WITH start_level AS (Select a.'מספר הפעולה' as action_num FROM actions AS a WHERE 1 = a.'מספר טכניקה' AND a.'סוג פעולה' = 'T' LIMIT 1)
        SELECT dateStart FROM Exercise JOIN start_level WHERE userId = {user} AND dateStart > '{START_LEVEL_DEFAULT}' AND actionId = start_level.action_num          
        """
        self.conn.cur.execute(query)
        start_date = self.conn.cur.fetchone()
        if start_date == None:
            return 0
        query = f"""
        SELECT l."שלב מס'", l.'תקופת השלב בימים '  FROM levels as l order by l."שלב מס'"
        """
        self.conn.cur.execute(query)
        level_days = self.conn.cur.fetchall()
        now_time = datetime.now()
        start_date = datetime.strptime(start_date[0], DATE_FORMAT)
        for level, days in level_days:
            if start_date + timedelta(days=days) < now_time:
                start_date = start_date + timedelta(days=days)
            else:
                break
        return level


        return should_be_unit[0] if type(should_be_unit) == tuple else should_be_unit

    def get_current_t(self, user):
        query = f"""
        SELECT T1, T2, T3, T4, T5 FROM Sheet1 as s JOIN App_user as a on a.username = s.acount WHERE a.id = {user}
        """
        self.conn.cur.execute(query)
        current_t = self.conn.cur.fetchone()
        if current_t == None or any([t == None for t in current_t]):
            return 0, 0, [0,0,0,0,0]
        current_t = [datetime.strptime(t, DATE_FORMAT[:8]) for t in current_t]
        now = datetime.now()
        for t_i, t in enumerate(current_t):
            if t > now:
                break
        return t_i+1, (t - now).days, [(t - now).days for t in current_t]

    def get_current_unit(self, user):
        query = f"""
        SELECT levelId FROM PositionLevel WHERE userId = {user}
        """
        self.conn.cur.execute(query)
        current_unit = self.conn.cur.fetchone()
        return current_unit[0] if type(current_unit) == tuple else current_unit


    def get_exercise_of_unit(self,unit):
        query = f"""
        SELECT a.'מספר הפעולה', a.'מספר סידורי' FROM actions as a 
        """
        self.conn.cur.execute(query)
        actions = self.conn.cur.fetchall()
        actions = [a[0] for a in actions if int(a[1].split(".")[0]) == unit]
        return actions

    def get_n_exercises(self, user, unit):
        actions = self.get_exercise_of_unit(unit)
        query = f"""
        SELECT count(*) FROM Exercise WHERE userId = {user} AND actionId  IN ({','.join(str(a) for a in actions)})
        """
        self.conn.cur.execute(query)
        n_exercises = self.conn.cur.fetchone()
        return n_exercises[0] if type(n_exercises) == tuple else n_exercises

    def get_time_since_starting_unit(self, user, unit):
        query = f"""
        SELECT dateStart FROM Exercise JOIN actions ON Exercise.actionId = actions.'מספר הפעולה' JOIN level_techniquesList ON actions.'מספר טכניקה' = level_techniquesList.'טכניקה'
         WHERE userId = {user} AND level_techniquesList.'טכניקה' = {unit} ORDER BY dateStart ASC LIMIT 1
        """
        self.conn.cur.execute(query)
        start_date = self.conn.cur.fetchone()
        if start_date == None:
            query = f"""
            SELECT startLevel FROM PositionLevel WHERE userId = {user} AND levelId = {unit}
            """
            self.conn.cur.execute(query)
            start_date = self.conn.cur.fetchone()
        if start_date == None:
            return -1, None
        start_date = datetime.strptime(start_date[0], DATE_FORMAT)
        now = datetime.now()
        return (now - start_date).days, start_date

    def get_last_exercise_date(self, user):
        query = f"""
            SELECT dateStart FROM Exercise WHERE userId = {user} ORDER BY dateStart DESC
        """
        self.conn.cur.execute(query)
        last_exercise_time = self.conn.cur.fetchone()

        if last_exercise_time == None:
            return None
        last_exercise_time = datetime.strptime(last_exercise_time[0], DATE_FORMAT)
        return last_exercise_time

    def get_percentage_done_of_unit(self, unit, n_exercises):
        actions = self.get_exercise_of_unit(unit)
        return n_exercises/len(actions)

    def number_of_days_in_unit(self, user, unit):
        actions = self.get_exercise_of_unit(unit)
        query = f"""
        SELECT dateStart FROM Exercise WHERE userId = {user} AND actionId  IN ({','.join([str(a) for a in actions])}) ORDER BY dateStart DESC LIMIT 1
        """
        self.conn.cur.execute(query)
        last_exercise_time = self.conn.cur.fetchone()
        if last_exercise_time == None:
            query = f"""
            SELECT startLevel FROM PositionLevel WHERE userId = {user} AND levelId = {unit}
            """
            self.conn.cur.execute(query)
            last_exercise_time = self.conn.cur.fetchone()
        if last_exercise_time == None:
            return -1
        last_exercise_time = datetime.strptime(last_exercise_time[0], DATE_FORMAT)
        now = datetime.now()
        return (now - last_exercise_time).days

    def get_min_n_exercises_for_unit(self, unit):
        actions = self.get_exercise_of_unit(unit)
        query = f"""
        WITH technique AS (SELECT distinct a.'מספר טכניקה' as technique FROM actions as a WHERE a.'מספר הפעולה' IN ({','.join([str(a) for a in actions])}))
        SELECT SUM(tech.'מינמום פעולות ') FROM technique t join techniques as tech on t.technique = tech.'מספר טכניקה'
        """
        self.conn.cur.execute(query)
        done_exercises = self.conn.cur.fetchone()
        return done_exercises[0]

    def get_min_n_days_for_unit(self, unit):
        query = f"""
        SELECT "תקופת השלב בימים " FROM levels WHERE "שלב מס'" = {unit}
        """
        self.conn.cur.execute(query)
        done_exercises = self.conn.cur.fetchone()
        return done_exercises[0]

    def get_x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales(self, user, x=4, y=2, z=2):
        query = f"""
            WITH last_x_sessions AS (
                SELECT * FROM Exercise WHERE userId = {user} ORDER BY dateStart DESC LIMIT {x}
            )
                SELECT q1.suds_stress < q2.suds_stress as suds, q1.fatigue < q2.fatigue as fatigue, q1.vas_pain < q2.vas_pain as vas
                FROM Questionnaire as q1, Questionnaire as q2, last_x_sessions as l
                WHERE l.questionnairePrimerId = q1.questionnaireId AND l.questionnaireLastId = q2.questionnaireId;
        """
        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        exercises = [list(e) for e in exercises if sum(e) >= z]
        return len(exercises) >= y

        # exercises = [list(e) for e in exercises]
        # exercises = np.array(exercises)
        # exercises = np.sum(exercises, axis=0)
        # return len(exercises[exercises >= z]) >= y

    def get_n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session(self, user, days):
        query = f"""
            SELECT endSession Is NULL FROM Session WHERE userId = {user} AND startSession > date('now', '-{days} days')
        """
        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        return sum([e[0] for e in exercises]) > 0

    def get_n_different_exercises_per_x_samples(self, user, samples):
        query = f"""
            SELECT COUNT(distinct actionId) FROM Exercise WHERE userId = {user} ORDER BY dateStart DESC LIMIT {samples}
        """
        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        return exercises[0][0]

    def get_n_exercieses_in_past_x_days(self, user, days):
        query = f"""
            SELECT COUNT(distinct actionId) FROM Exercise WHERE userId = {user} AND dateStart > date('now', '-{days} days')
        """
        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        return exercises[0][0]

    def get_trend(self, user, mesurements=14):
        query = f"""
            WITH 
            last_x_mesurements AS (SELECT * FROM Exercise WHERE userId = {user} ORDER BY dateStart DESC LIMIT {mesurements})
            SELECT q1.suds_stress AS sudsQ1, q1.fatigue AS fatigueQ1, q1.vas_pain AS vasQ1, q2.suds_stress AS sudsQ2, q2.fatigue AS fatigueQ2, q2.vas_pain AS vasQ2
            FROM last_x_mesurements as l JOIN Questionnaire as q1 on l.questionnairePrimerId = q1.questionnaireId
            JOIN Questionnaire as q2 on l.questionnaireLastId = q2.questionnaireId
            ORDER BY l.dateStart DESC
            
        """
        self.conn.cur.execute(query)
        exercises = self.conn.cur.fetchall()
        exercises = [list(e) for e in exercises]
        exercises = np.array(exercises)

        if len(exercises) < 2:
            # print("not enough exercises")
            return MISC, 0, '0-0'

        sudsQ1_regression =np.array([e[0] for e in exercises])
        fatigueQ1_regression = np.array([e[1] for e in exercises])
        vasQ1_regression = np.array([e[2] for e in exercises])
        sudsQ2_regression = np.array([e[3] for e in exercises])
        fatigueQ2_regression = np.array([e[4] for e in exercises])
        vasQ2_regression = np.array([e[5] for e in exercises])

        sudsQ1slope, sudsQ1intercept, sudsQ1r_value, sudsQ1p_value, sudsQ1std_err = linregress(
            # list(range(len(sudsQ1_regression))), sudsQ1_regression
            [0, len(sudsQ1_regression)], [sudsQ1_regression[0], sudsQ1_regression.mean()]
        )
        fatigueQ1slope, fatigueQ1intercept, fatigueQ1r_value, fatigueQ1p_value, fatigueQ1std_err = linregress(
            # list(range(len(fatigueQ1_regression))), fatigueQ1_regression
            [0, len(fatigueQ1_regression)], [fatigueQ1_regression[0], fatigueQ1_regression.mean()]
        )
        vasQ1slope, vasQ1intercept, vasQ1r_value, vasQ1p_value, vasQ1std_err = linregress(
            # list(range(len(vasQ1_regression))), vasQ1_regression
            [0, len(vasQ1_regression)], [vasQ1_regression[0], vasQ1_regression.mean()]
        )
        sudsQ2slope, sudsQ2intercept, sudsQ2r_value, sudsQ2p_value, sudsQ2std_err = linregress(
            # list(range(len(sudsQ2_regression))), sudsQ2_regression
            [0, len(sudsQ2_regression)], [sudsQ2_regression[0], sudsQ2_regression.mean()]
        )
        fatigueQ2slope, fatigueQ2intercept, fatigueQ2r_value, fatigueQ2p_value, fatigueQ2std_err = linregress(
            # list(range(len(fatigueQ2_regression))), fatigueQ2_regression
            [0, len(fatigueQ2_regression)], [fatigueQ2_regression[0], fatigueQ2_regression.mean()]
        )
        vasQ2slope, vasQ2intercept, vasQ2r_value, vasQ2p_value, vasQ2std_err = linregress(
            # list(range(len(vasQ2_regression))), vasQ2_regression
            [0, len(vasQ2_regression)], [vasQ2_regression[0], vasQ2_regression.mean()]
        )

        # use plotly to plot the regression line
        # import plotly.graph_objects as go
        # fig = go.Figure()
        # fig.add_trace(go.Scatter(x=list(range(len(sudsQ1_regression))), y=sudsQ1_regression, mode='markers', name='sudsQ1'))
        # fig.add_trace(go.Scatter(x=list(range(len(sudsQ1_regression))), y=sudsQ1slope*np.array(list(range(len(sudsQ1_regression))))+sudsQ1intercept, mode='lines', name='sudsQ1'))
        # fig.show()

        suds_trend = None
        if sudsQ1slope <= 0 and sudsQ2slope <= 0 :
            q1_regreesion = sudsQ1slope*(len(sudsQ1_regression)-1) + sudsQ1intercept
            q2_regreesion = sudsQ2slope*(len(sudsQ2_regression)-1) + sudsQ2intercept
            if  q1_regreesion-q2_regreesion > 1:
                suds_trend = INC
            elif 1 >= q1_regreesion-q2_regreesion >= 0:
                suds_trend = STAG
            else:
                suds_trend = MISC
        else:
            suds_trend = DET
        before_suds_within = self.score_to_CS(sudsQ1_regression[0]) - self.score_to_CS(sudsQ1_regression[-1])
        before_suds =f"{self.score_to_CS(sudsQ1_regression[0])}-{self.score_to_CS(sudsQ1_regression[-1])}"
        after_suds_within = self.score_to_CS(sudsQ2_regression[0]) - self.score_to_CS(sudsQ2_regression[-1])
        after_suds = f"{self.score_to_CS(sudsQ2_regression[0])}-{self.score_to_CS(sudsQ2_regression[-1])}"

        fatigue_trend = None
        if fatigueQ1slope <= 0 and fatigueQ2slope <= 0 :
            q1_regreesion = fatigueQ1slope*(len(fatigueQ1_regression)-1) + fatigueQ1intercept
            q2_regreesion = fatigueQ2slope*(len(fatigueQ2_regression)-1) + fatigueQ2intercept
            if q1_regreesion - q2_regreesion > 1:
                fatigue_trend = INC
            elif 1 >= q1_regreesion - q2_regreesion >= 0:
                fatigue_trend = STAG
            else:
                fatigue_trend = MISC
        else:
            fatigue_trend = DET
        before_fatigue_within = self.score_to_CS(fatigueQ1_regression[0]) - self.score_to_CS(fatigueQ1_regression[-1])
        before_fat = f"{self.score_to_CS(fatigueQ1_regression[0])}-{self.score_to_CS(fatigueQ1_regression[-1])}"
        after_fatigue_within = self.score_to_CS(fatigueQ2_regression[0]) - self.score_to_CS(fatigueQ2_regression[-1])
        after_fat = f"{self.score_to_CS(fatigueQ2_regression[0])}-{self.score_to_CS(fatigueQ2_regression[-1])}"


        vas_trend = None
        if vasQ1slope <= 0 and vasQ2slope <= 0 :
            q1_regreesion = vasQ1slope*(len(vasQ1_regression)-1) + vasQ1intercept
            q2_regreesion = vasQ2slope*(len(vasQ2_regression)-1) + vasQ2intercept
            if q1_regreesion - q2_regreesion > 1:
                vas_trend = INC
            elif 1 >= q1_regreesion - q2_regreesion >= 0:
                vas_trend = STAG
            else:
                vas_trend = MISC
        else:
            vas_trend = DET
        before_vas_within = self.score_to_CS(vasQ1_regression[0]) - self.score_to_CS(vasQ1_regression[-1])
        before_vas = f"{self.score_to_CS(vasQ1_regression[0])}-{self.score_to_CS(vasQ1_regression[-1])}"
        after_vas_within = self.score_to_CS(vasQ2_regression[0]) - self.score_to_CS(vasQ2_regression[-1])
        after_vas = f"{self.score_to_CS(vasQ2_regression[0])}-{self.score_to_CS(vasQ2_regression[-1])}"

        if suds_trend == INC or fatigue_trend == INC or vas_trend == INC:
            sum_trend = [True]
            trends_num = [before_vas_within, before_fatigue_within, before_suds_within, after_suds_within, after_fatigue_within, after_vas_within]
            trends_string = [before_suds, before_fat, before_vas, after_suds, after_fat, after_vas]
            if suds_trend == INC:
                sum_trend.append(before_suds_within==0 and after_suds_within==0)
            if fatigue_trend == INC:
                sum_trend.append(before_fatigue_within==0 and after_fatigue_within==0)
            if vas_trend == INC:
                sum_trend.append(before_vas_within==0 and after_vas_within==0)
            return INC, all(sum_trend), trends_string[np.argmax(trends_num)]
        if suds_trend == STAG or fatigue_trend == STAG or vas_trend == STAG:
            sum_trend = [True]
            trends_num = [before_vas_within, before_fatigue_within, before_suds_within, after_suds_within, after_fatigue_within, after_vas_within]
            trends_string = [before_suds, before_fat, before_vas, after_suds, after_fat, after_vas]
            if suds_trend == STAG:
                sum_trend.append(before_suds_within==0 and after_suds_within==0)
            if fatigue_trend == STAG:
                sum_trend.append(before_fatigue_within==0 and after_fatigue_within==0)
            if vas_trend == STAG:
                sum_trend.append(before_vas_within==0 and after_vas_within==0)
            return STAG, all(sum_trend), trends_string[np.argmax(trends_num)]
        if suds_trend == DET and fatigue_trend == DET and vas_trend == DET:
            sum_trend = [True]
            trends_num = [before_vas_within, before_fatigue_within, before_suds_within, after_suds_within, after_fatigue_within, after_vas_within]
            trends_string = [before_suds, before_fat, before_vas, after_suds, after_fat, after_vas]

            if suds_trend == DET:
                sum_trend.append(before_suds_within==0 and after_suds_within==0)
            if fatigue_trend == DET:
                sum_trend.append(before_fatigue_within==0 and after_fatigue_within==0)
            if vas_trend == DET:
                sum_trend.append(before_vas_within==0 and after_vas_within==0)
            return DET, all(sum_trend), trends_string[np.argmin(trends_num)]
        return MISC, 0, '0-0'




        exercises = np.sum(exercises, axis=0)
        trend = np.argmax(exercises)
        return trend, self.score_to_CS(exercises[0]), self.score_to_CS(exercises[1])

    def get_last_time_message(self, user, e):
        query = f"""
        SELECT date FROM History_bots WHERE userId = {user} AND messageId = {e} ORDER BY date DESC LIMIT 1
        """
        self.conn.cur.execute(query)
        last_time = self.conn.cur.fetchone()
        last_time = datetime.strptime(last_time[0], DATE_FORMAT)
        return (datetime.now() - last_time).days

    def is_real_user(self, user):
        query = f"""
        SELECT COUNT(*) FROM App_user as a JOIN Sheet1 as s ON s.acount = a.username WHERE a.id = {user}
        """
        self.conn.cur.execute(query)
        real_user = self.conn.cur.fetchone()
        if real_user is None:
            return False
        return real_user[0] > 0


    def get_messages(self):

        exercise_priority_message = [-1]+ [63, 24, 25, 26, 27, 76] +list(range(12,24)) + [66,67,68,69,70,71,72,73,74] + list(range(1, 11)) + list(range(28,66))
        exercise_message_interval =({i: 2 for i in [63, 24, 25, 26, 27, 76] +list(range(12,24)) + [66,67,68,69,70,71,72,73,74] + list(range(1, 11))} | {8:4, 9:4} |
                                    {i: 2 for i in range(28,47)}) | {47:7, 48:7, 49: 4*7} | {i: 6 for i in range(50, 66)} | {-1:0} | {31:1000, 32: 1000}


        uesr_to_message = {}

        users = self.get_users()
        for user in users:
            user = user[0]
            real_user = self.is_real_user(user)
            if not real_user:
                continue
            uesr_to_message[user] = []
            user_sex = self.get_user_sex(user)

            last_exercise_time = self.get_last_exercise_date(user)
            if last_exercise_time == None:
                print(user, "bad: no exercise")
                uesr_to_message[user].append(-1)
                continue

            trend, within_or_between, cs = self.get_trend(user)
            if trend == MISC:
                print(user, "bad: no valid trend")
                uesr_to_message[user].append(-1)
                continue

            current_t, x_days_from_current_t, x_days_from_T_is = self.get_current_t(user)

            p_value = current_t if current_t < 5 else 4
            if p_value == 0:
                print(user, "bad: no T value")
                uesr_to_message[user].append(-1)
                continue



            should_be_unit = self.get_should_be_unit(user)
            current_unit = self.get_current_unit(user)
            n_exercises_from_current_unit = self.get_n_exercises(user, current_unit)
            time_since_starting_unit, date_of_starting_unit = self.get_time_since_starting_unit(user, current_unit)
            percentage_done_of_unit = self.get_percentage_done_of_unit(current_unit, n_exercises_from_current_unit)
            number_of_days_in_unit = self.number_of_days_in_unit(user, current_unit)
            min_n_exercises_for_unit = self.get_min_n_exercises_for_unit(current_unit)
            min_n_days_for_unit = self.get_min_n_days_for_unit(current_unit)
            x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales = self.get_x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales(user, x=4, y=2, z=2)
            n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session = self.get_n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session(user, current_t)
            n_different_exercises_per_x_samples = self.get_n_different_exercises_per_x_samples(user, 14)
            n_exercieses_in_past_x_days = self.get_n_exercieses_in_past_x_days(user, 7)

            if p_value == 1:
                if (datetime.now() - last_exercise_time).days == 3 and (trend in [INC, STAG]) and within_or_between == WITHIN:
                    uesr_to_message[user].append(1)
                if (datetime.now() - last_exercise_time).days == 3 and (trend in [INC, STAG]) and within_or_between == BETWEEN:
                    uesr_to_message[user].append(2)
                if (datetime.now() - last_exercise_time).days == 3 and (trend in [DET]) and within_or_between == WITHIN:
                    uesr_to_message[user].append(10)
                if (datetime.now() - last_exercise_time).days == 3 and (trend in [DET]) and within_or_between == BETWEEN:
                    uesr_to_message[user].append(11)
                if 4 <= (datetime.now() - last_exercise_time).days <= 14 and (trend in [INC]):
                    uesr_to_message[user].append(3)
                if 4 <= (datetime.now() - last_exercise_time).days <= 14 and (trend in [STAG]):
                    uesr_to_message[user].append(4)
                if 4 <= (datetime.now() - last_exercise_time).days <= 14 and (trend in [DET]):
                    uesr_to_message[user].append(5)
                if 15 <= (datetime.now() - last_exercise_time).days <= 22:
                    uesr_to_message[user].append(6)
                if 23 <= (datetime.now() - last_exercise_time).days <= 29:
                    uesr_to_message[user].append(7)
                if 30 <= (datetime.now() - last_exercise_time).days <= 60:
                    uesr_to_message[user].append(8)
                if 60 <= (datetime.now() - last_exercise_time).days <= 90:
                    uesr_to_message[user].append(9)
    
                if current_unit == 1 and number_of_days_in_unit >= 3 and n_exercises_from_current_unit <= 1:
                    uesr_to_message[user].append(66)
                if current_unit == 1 and number_of_days_in_unit >= 5 and n_exercises_from_current_unit <= 2:
                    uesr_to_message[user].append(67)
                if current_unit == 2 and number_of_days_in_unit >= 3 and n_exercises_from_current_unit <= 1:
                    uesr_to_message[user].append(68)
                if current_unit == 2 and number_of_days_in_unit >= 5 and n_exercises_from_current_unit <= 2:
                    uesr_to_message[user].append(69)
                if current_unit == 3 and number_of_days_in_unit >= 2 and n_exercises_from_current_unit <= 1:
                    uesr_to_message[user].append(70)
                if current_unit == 3 and number_of_days_in_unit >= 4 and n_exercises_from_current_unit <= 2:
                    uesr_to_message[user].append(71)
                if current_unit == 4 and number_of_days_in_unit >= 3 and n_exercises_from_current_unit <= 1:
                    uesr_to_message[user].append(72)
                if current_unit == 4 and number_of_days_in_unit >= 7 and n_exercises_from_current_unit <= 2:
                    uesr_to_message[user].append(73)
                if current_unit == 4 and number_of_days_in_unit >= 14 and n_exercises_from_current_unit <= 3:
                    uesr_to_message[user].append(74)
                if should_be_unit == 2 and current_unit == 1 and trend in [INC, STAG]:
                    uesr_to_message[user].append(12)
                if should_be_unit == 2 and current_unit == 2 and trend in [DET]:
                    uesr_to_message[user].append(13)
                if should_be_unit == 3 and current_unit == 1 and trend in [INC, STAG]:
                    uesr_to_message[user].append(14)
                if should_be_unit == 3 and current_unit == 1 and trend in [DET]:
                    uesr_to_message[user].append(15)
                if should_be_unit == 3 and current_unit == 2 and trend in [INC, STAG]:
                    uesr_to_message[user].append(16)
                if should_be_unit == 3 and current_unit == 2 and trend in [DET]:
                    uesr_to_message[user].append(17)
                if should_be_unit == 4 and current_unit == 1 and trend in [INC, STAG]:
                    uesr_to_message[user].append(18)
                if should_be_unit == 4 and current_unit == 1 and trend in [DET]:
                    uesr_to_message[user].append(19)
                if should_be_unit == 4 and current_unit == 2 and trend in [INC, STAG]:
                    uesr_to_message[user].append(20)
                if should_be_unit == 4 and current_unit == 2 and trend in [DET]:
                    uesr_to_message[user].append(21)
                if should_be_unit == 4 and current_unit == 3 and trend in [INC, STAG]:
                    uesr_to_message[user].append(22)
                if should_be_unit == 4 and current_unit == 3 and trend in [DET]:
                    uesr_to_message[user].append(23)
                if x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales:
                    uesr_to_message[user].append(76)
                if min_n_days_for_unit > number_of_days_in_unit[1] and  current_unit in [1,2,3]:
                    uesr_to_message[user].append(24)
                if min_n_days_for_unit > number_of_days_in_unit[1] and current_unit in [4]:
                    uesr_to_message[user].append(25)
                if current_unit == 5:
                    uesr_to_message[user].append(26)
                if current_unit == 4 and percentage_done_of_unit < 0.8*min_n_exercises_for_unit:
                    uesr_to_message[user].append(27)
            elif p_value > 1:
                if p_value==2 and x_days_from_T_is[3-1] >= 6*30:
                    uesr_to_message[user].append(28)
                if p_value==3 and x_days_from_T_is[4-1] >= 9*30:
                    uesr_to_message[user].append(29)
                if p_value==4 and x_days_from_T_is[5-1] >= 12*30:
                    uesr_to_message[user].append(30)

                if p_value==2 and current_unit == 5 and (date_of_starting_unit- last_exercise_time).days > 7  and trend in [INC, STAG]:
                    uesr_to_message[user].append(31)
                if p_value == 2 and current_unit == 5 and  (date_of_starting_unit- last_exercise_time).days > 7 and trend in [DET]:
                    uesr_to_message[user].append(32)
                if p_value > 1 and (datetime.now() - last_exercise_time).days >= 3 and x_days_from_current_t == 3:
                    uesr_to_message[user].append(33)
                if p_value > 1 and 7 > (datetime.now() - last_exercise_time).days >= 3:
                    uesr_to_message[user].append(34)

                if p_value > 1 and (datetime.now() - last_exercise_time).days == 7 and trend in [INC, STAG] and cs == "1-1":
                    uesr_to_message[user].append(35)
                if p_value > 1 and (datetime.now() - last_exercise_time).days == 7 and trend in [INC, STAG] and cs == "2-2":
                    uesr_to_message[user].append(36)
                if p_value > 1 and (datetime.now() - last_exercise_time).days == 7 and trend in [INC, STAG] and cs == "3-3":
                    uesr_to_message[user].append(37)
                if p_value > 1 and (datetime.now() - last_exercise_time).days == 7 and trend in [INC] and cs in ["3-1", "3-2"]:
                    uesr_to_message[user].append(38)
                if p_value > 1 and (datetime.now() - last_exercise_time).days == 7 and trend in [INC] and cs in ["2-1"]:
                    uesr_to_message[user].append(39)
                if p_value > 1 and (datetime.now() - last_exercise_time).days == 7 and trend in [DET]:
                    if cs in ["1-1"]:
                        uesr_to_message[user].append(40)
                    if cs in ["2-2"]:
                        uesr_to_message[user].append(41)
                    if cs in ["3-3"]:
                        uesr_to_message[user].append(42)
                    if cs in ["2-3", "1-3"]:
                        uesr_to_message[user].append(43)
                    if cs in ["1-2"]:
                        uesr_to_message[user].append(44)

                if p_value > 1 and 21 >= (datetime.now() - last_exercise_time).days >= 14:
                    uesr_to_message[user].append(45)
                if p_value > 1 and 28 >= (datetime.now() - last_exercise_time).days >= 21:
                    uesr_to_message[user].append(46)
                if p_value > 1 and 8*7 >= (datetime.now() - last_exercise_time).days >= 28:
                    uesr_to_message[user].append(47)
                if p_value > 1 and 12*7 >= (datetime.now() - last_exercise_time).days >= 8*7:
                    uesr_to_message[user].append(48)
                if p_value > 1 and (datetime.now() - last_exercise_time).days >= 12*7:
                    uesr_to_message[user].append(49)


                if p_value>1 and n_different_exercises_per_x_samples <= 3:
                    if trend == STAG:
                        if n_exercieses_in_past_x_days <= 3:
                            if cs == "1-1":
                                uesr_to_message[user].append(50)
                            if cs == "2-2" or cs == "3-3":
                                uesr_to_message[user].append(51)
                        else:
                            if cs == "1-1":
                                uesr_to_message[user].append(52)
                            if cs == "2-2" or cs == "3-3":
                                uesr_to_message[user].append(53)
                    if trend == INC:
                        if n_exercieses_in_past_x_days <= 3:
                            if cs == "1-1":
                                uesr_to_message[user].append(54)
                            if cs == "2-2" or cs == "3-3":
                                uesr_to_message[user].append(55)
                            if cs == "3-1" or cs == "2-1":
                                uesr_to_message[user].append(56)
                            if cs == "3-2":
                                uesr_to_message[user].append(57)
                        else:
                            if cs == "1-1":
                                uesr_to_message[user].append(58)
                            if cs == "2-2" or cs == "3-3":
                                uesr_to_message[user].append(59)
                            if cs == "3-1" or cs == "2-1":
                                uesr_to_message[user].append(60)
                            if cs == "3-2":
                                uesr_to_message[user].append(61)
                    if trend == DET:
                        if n_exercieses_in_past_x_days <= 3:
                            if cs[0]==cs[-1]:
                                uesr_to_message[user].append(62)
                            else:
                                uesr_to_message[user].append(63)
                        else:
                            if cs[0]==cs[-1]:
                                uesr_to_message[user].append(64)
                            else:
                                uesr_to_message[user].append(65)
                if n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session:
                    uesr_to_message[user].append(75)


        user_indexes = {}
        for user in uesr_to_message:
            # exercises = []
            # for e in uesr_to_message[user]:
            #     last = self.get_last_time_message(user, e)
            #     if last > exercise_message_interval[e]:
            #         exercises.append(e)
            # uesr_to_message[user] = exercises
            indexes = sorted(uesr_to_message[user], key=lambda x: exercise_priority_message.index(x))
            uesr_to_message[user] = [self.id_to_message(id_, user_sex) for id_ in indexes]
            user_indexes[user] = indexes
        return uesr_to_message, user_indexes

