from datetime import datetime, timedelta
import json
from typing import Optional
import random

from src.heuristics.queries import EXERCISES_IN_PROGRESS_QUERY, USER_LEVEL_QUERY, NON_IMPROVED_QUERY, USER_WHERE, \
    CURRENT_PROGRESS_QUERY, USER_TO_FINISHED_EXERCISES, ACTIONS_NOT_DONE_QUERY, AVAILABLE_ACTIONS_QUERY, \
    TRAINING_DATA_QUERY
from src.utils.session_object import Session
import numpy as np

FREE_USER_LEVEL = 5
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
NOW= "now" #"2023-07-13 14:39:58" #
ESCILATION_FEATURE_DATE = "2024-07-15 14:39:58"
DAYS = 14

class HumanExpert:
    def __init__(self, conn):
        self.conn = conn

    def get_current_progress(self) -> Optional[Session]:
        query = """
        SELECT userId, levelId
        FROM PositionLevel
        WHERE (userId, startLevel) IN (
            SELECT userId, MAX(startLevel)
            FROM PositionLevel
            GROUP BY userId
        ) AND levelID > 1;
        """
        # Execute the query
        self.conn.cur.execute(query)

        # Fetch the results
        user_levels = self.conn.cur.fetchall()
        return user_levels

    def get_users_exercise_in_past_k(self, user_id: int, days=14, to_day = 0) -> int:
        # get all excerises in the past k days for each user. per usuer aggrirgate in a list
        query = f"""
            SELECT Exercise.exerciseId, actions.'מספר טכניקה'
            FROM Exercise JOIN actions ON Exercise.actionId = actions.'מספר הפעולה'
            WHERE Exercise.userId = {user_id} AND Exercise.dateSTART >= DATE('{NOW}', '-{days} days')
            """ #  AND Exercise.dateSTART >= DATE('{NOW}', '-{to_day} days')
        # Execute the query
        self.conn.cur.execute(query)
        # Fetch the results
        exercise_of_user = self.conn.cur.fetchall()
        return exercise_of_user

    def get_users(self):
        query = USER_LEVEL_QUERY
        self.conn.cur.execute(query)
        users = self.conn.cur.fetchall()
        return users

    def get_sessions(self, user_id, days=14, to_day = 0):
        query = f"""
            SELECT sudsQ1, sudsQ2, vasQ1, vasQ2, fatigueQ1, fatigueQ2
            FROM Session
            WHERE userId = {user_id} AND Session.startSession >= DATE('{NOW}', '-{days} days')
            ORDER BY startSession DESC
            """ #  AND Session.startSession >= DATE('{NOW}', '-{to_day} days')
        self.conn.cur.execute(query)
        sessions = self.conn.cur.fetchall()
        return sessions

    def get_latest_recommendation(self):
        query = f"""
            SELECT userID, MAX(date) as most_recent_date
            FROM History_bots
            WHERE date >= '{ESCILATION_FEATURE_DATE}' AND actions != '[]' AND actions IS NOT NULL
            GROUP BY userID
        """
        self.conn.cur.execute(query)
        latest_recommendation = self.conn.cur.fetchall()
        return latest_recommendation


    def recommend(self):
        last_recommendation = self.get_latest_recommendation()
        last_recommendation = {k:datetime.strptime(v, DATE_FORMAT) for k,v in last_recommendation}
        three_days_ago = (datetime.now() - timedelta(days=3)).date()

        non_var_non_increasing, non_var_non_increasing = self.need_expert(days=14)
        _, escilation1 = self.need_expert(days=14+1*3)
        escilation1 = {k:v for k,v in escilation1.items() if len(v) > 0 and last_recommendation.get(k, datetime(2000, 1, 1)).date() == three_days_ago}
        _, escilation2 = self.need_expert(days=14+2*3)
        escilation2 = {k:v for k,v in escilation2.items() if len(v) > 0 and last_recommendation.get(k, datetime(2000, 1, 1)).date() == three_days_ago}
        _, escilation3 = self.need_expert(days=14+3*3)
        escilation3 = {k:v for k,v in escilation3.items() if len(v) > 0 and last_recommendation.get(k, datetime(2000, 1, 1)).date() == three_days_ago}

        escilation2 = {k:v for k,v in escilation2.items() if k not in escilation3}
        escilation1 = {k:v for k,v in escilation1.items() if k not in escilation2 and k not in escilation3}
        non_var_non_increasing = {k:v for k,v in non_var_non_increasing.items() if k not in escilation1 and k not in escilation2 and k not in escilation3}

        user_recommendations = {
            user[0]: [] for user in self.get_users()
        }
        for user in non_var_non_increasing:
            exercises = non_var_non_increasing[user][0]
            techniques = non_var_non_increasing[user][1]
            suds_before = non_var_non_increasing[user][2]
            vas_before = non_var_non_increasing[user][3]
            fatigue_before = non_var_non_increasing[user][4]
            last_cs_before = self.get_CS(max(suds_before[-1], vas_before[-1], fatigue_before[-1]))

            rule1_recommendations = self.rule1_recommendation(user, exercises, techniques)
            rule2_recommendations = self.rule2_recommendation(user, techniques, 1)
            rule2_recommendations = [[r, 2] for r in rule2_recommendations]
            rule3_recommendations = self.rule3_recommendation(user)

            if last_cs_before == 2 and len(rule2_recommendations) == 0:
                rule2_recommendations = self.rule2_recommendation(user, techniques, 2)
                rule2_recommendations = [[r, 7] for r in rule2_recommendations]
            if last_cs_before == 3 and len(rule2_recommendations) == 0:
                rule2_recommendations = self.rule2_recommendation(user, techniques, 3)
                rule2_recommendations = [[r, 8] for r in rule2_recommendations]
            recommendations = [[r[0], 1] for r in rule1_recommendations] + [[r[0], 3] for r in rule3_recommendations] + rule2_recommendations
            user_recommendations[user] = recommendations

        for user in escilation1:
            rule4_recommendations = self.find_positive_trend(user)
            user_recommendations[user] += rule4_recommendations
        for user in escilation2:
            rule5_recommendations = self.recommend_new_exercises(user)
            user_recommendations[user] += rule5_recommendations
        for user in escilation3:
            rule6_recommendations = self.get_all_exercises_randomized()
            user_recommendations[user] += rule6_recommendations
        return user_recommendations

    def get_all_exercises_randomized(self):
        # Query to get all available exercises
        query_all = """
            SELECT DISTINCT actions.'מספר הפעולה'
            FROM actions
        """
        self.conn.cur.execute(query_all)
        all_exercises = [row[0] for row in self.conn.cur.fetchall()]

        # Randomize the order of the exercises
        random.shuffle(all_exercises)

        return all_exercises
    def recommend_new_exercises(self, user_id):
        # Query to get all exercises done by the user
        query_done = f"""
            SELECT DISTINCT Exercise.actionId
            FROM Exercise
            WHERE Exercise.userId = {user_id}
        """
        self.conn.cur.execute(query_done)
        done_exercises = {row[0] for row in self.conn.cur.fetchall()}

        # Query to get all available exercises
        query_all = """
            SELECT DISTINCT actions.'מספר הפעולה'
            FROM actions
        """
        self.conn.cur.execute(query_all)
        all_exercises = {row[0] for row in self.conn.cur.fetchall()}

        # Find exercises that have not been done by the user
        new_exercises = all_exercises - done_exercises

        return list(new_exercises)

    def find_positive_trend(self, user_id, days=14):
        query = f"""
            SELECT Exercise.actionId, sudsQ1, vasQ1, fatigueQ1, sudsQ2, vasQ2, fatigueQ2
            FROM Session 
            JOIN list_exercises_ids ON Session.sessionId = list_exercises_ids.sessionId
            JOIN Exercise ON list_exercises_ids.list_exercises_ids = Exercise.exerciseId
            JOIN actions ON Exercise.actionId = actions.'מספר הפעולה'
            WHERE Session.userId = {user_id} AND Session.startSession >= DATE('{NOW}', '-{days} days')
            ORDER BY Session.startSession ASC
        """
        self.conn.cur.execute(query)
        all_exercises = self.conn.cur.fetchall()

        before_change = {}
        after_change = {}
        for exercise, suds1, vas1, fat1, suds2, vas2, fat2 in all_exercises:
            cs_before = max(suds1, vas1, fat1)
            cs_after = max(suds2, vas2, fat2)
            if exercise not in before_change: before_change[exercise] = []
            if exercise not in after_change: after_change[exercise] = []
            before_change[exercise].append(cs_before)
            after_change[exercise].append(cs_after)

        # for each exercise, get the longest negative trend only looking at the befores
        trend_changes = {}
        for exercise in before_change:
            longest_before = self.longest_non_increasing_sequence(before_change[exercise])
            longest_after = self.longest_non_increasing_sequence(after_change[exercise])
            longest = longest_before
            if len(longest_after) > len(longest_before):
                longest = longest_after
            if len(longest) > 10:
                trend_changes[exercise] = longest

        trends_changes = sorted(trend_changes.items(), key=lambda x: len(x[1]), reverse=True)
        return trends_changes


    def longest_non_increasing_sequence(self, numbers):
        if not numbers: return []
        longest_seq = []
        current_seq = [numbers[0]]
        for i in range(1, len(numbers)):
            if numbers[i] <= numbers[i - 1]:
                current_seq.append(numbers[i])
            else:
                if len(current_seq) > len(longest_seq):
                    longest_seq = current_seq
                current_seq = [numbers[i]]
        if len(current_seq) > len(longest_seq):
            longest_seq = current_seq
        return longest_seq

    def need_expert(self, days=14, to_days=0):
        current_progress = self.get_current_progress()
        users = [uid for uid, level in current_progress]
        no_variaty_users = {}
        all_use_exc_and_tech = {}
        for user in users:
            exercise_of_user = self.get_users_exercise_in_past_k(user, days, to_days)
            exercise = [e[0] for e in exercise_of_user]
            techniques_of_user = [e[1] for e in exercise_of_user]
            all_use_exc_and_tech[user] = (exercise, techniques_of_user)
            if len(techniques_of_user) <= 3:
                # TODO this is strange to me, since 3 tehcniques can still be 9 exercises, which is alot.
                no_variaty_users[user] = [exercise, techniques_of_user]
        non_increasing_trend_user = {}

        for user in all_use_exc_and_tech:
            sessions_of_users = self.get_sessions(user, days)
            if len(sessions_of_users) == 0:
                # TODO undersand what to do in this situation?!
                continue
            suds_before = [s[0] for s in sessions_of_users]
            suds_after = [s[1] for s in sessions_of_users]
            vas_before = [s[2] for s in sessions_of_users]
            vas_after = [s[3] for s in sessions_of_users]
            fatigue_before = [s[4] for s in sessions_of_users]
            fatigue_after = [s[5] for s in sessions_of_users]
            suds_delta = np.array(suds_after) - np.array(suds_before)
            vas_delta = np.array(vas_after) - np.array(vas_before)
            fatigue_delta = np.array(fatigue_after) - np.array(fatigue_before)

            x1, y1, x2, y2 = 0, 0, min(days, len(sessions_of_users)), 2
            bad_slope = (y2 - y1) / (x2 - x1)

            range = np.arange(len(suds_before))
            if len(range) > 1:
                suds_before = np.array(list(reversed(suds_before)))
                suds_slope, suds_intercept = np.polyfit(range, suds_before, 1)
                vas_before = np.array(list(reversed(vas_before)))
                vas_slope, vas_intercept = np.polyfit(range, vas_before, 1)
                fatigue_before = np.array(list(reversed(fatigue_before)))
                fatigue_slope, fatigue_intercept = np.polyfit(range, fatigue_before, 1)
            else:
                suds_slope = vas_slope = fatigue_slope = bad_slope - 1

            if sum([suds_slope < bad_slope, vas_slope < bad_slope, fatigue_slope < bad_slope]) >= 2:
                non_increasing_trend_user[user] = (
                all_use_exc_and_tech[user][0], all_use_exc_and_tech[user][1], suds_before, vas_before, fatigue_before)

            bad_suds_delta = suds_delta <= -1
            bad_suds = sum(bad_suds_delta) >= 3
            bad_vas_delta = vas_delta <= -1
            bad_vas = sum(bad_vas_delta) >= 3
            bad_fatigue_delta = fatigue_delta <= -1
            bad_fatigue = sum(bad_fatigue_delta) >= 3
            if sum([bad_suds, bad_vas, bad_fatigue]) >= 2:
                non_increasing_trend_user[user] = (all_use_exc_and_tech[user][0], all_use_exc_and_tech[user][1], suds_before, vas_before, fatigue_before)
        non_var_non_increasing = {k:v for k,v in non_increasing_trend_user.items() if k in no_variaty_users}
        return non_var_non_increasing, non_increasing_trend_user

    def rule3_recommendation(self, user_id, days = 14):
        # get all exercises from the begning to until days ago.
        query = f"""
            SELECT Exercise.actionId, sudsQ1, vasQ1, fatigueQ1, sudsQ2, vasQ2, fatigueQ2, Session.startSession >= DATE('{NOW}', '-{days} days')  as in_last_days
            FROM Session JOIN  list_exercises_ids ON Session.sessionId = list_exercises_ids.sessionId
            JOIN Exercise ON list_exercises_ids.list_exercises_ids = Exercise.exerciseId
            JOIN actions ON Exercise.actionId = actions.'מספר הפעולה'
            WHERE Session.userId = {user_id} 
            """
        self.conn.cur.execute(query)
        all_exercises = self.conn.cur.fetchall()
        not_last_days_list = []
        in_last_days_list = []
        bad_last_days = []
        for exercise, suds1, vas1, fat1, suds2, vas2, fat2, in_last_days in all_exercises:
            if in_last_days:
                in_last_days_list.append((exercise, suds1, vas1, fat1, suds2, vas2, fat2))
            else:
                not_last_days_list.append((exercise, suds1, vas1, fat1, suds2, vas2, fat2))
        exercise_last_days_made_worse = {}
        for exercise, suds1, vas1, fat1, suds2, vas2, fat2 in in_last_days_list:
            if max(suds2 - suds1, vas2 - vas1, fat2 - fat1) > 0:
                exercise_last_days_made_worse[exercise] = max(suds2 - suds1, vas2 - vas1, fat2 - fat1)
        exercise_not_last_days_made_better = {}
        for exercise, suds1, vas1, fat1, suds2, vas2, fat2 in not_last_days_list:
            suds1, vas1, fat1, suds2, vas2, fat2 = suds1+1, vas1+1, fat1+1, suds2+1, vas2+1, fat2+1
            if exercise in exercise_last_days_made_worse: continue
            if exercise not in exercise_not_last_days_made_better: exercise_not_last_days_made_better[exercise] = []
            relative_delta = max((suds2 - suds1)/suds1, (vas2 - vas1)/vas1, (fat2 - fat1)/fat1)
            if relative_delta > 0: continue
            exercise_not_last_days_made_better[exercise].append(relative_delta)
        exercise_not_last_days_made_better = {k:v for k,v in exercise_not_last_days_made_better.items() if len(v) > 0}
        for e in exercise_not_last_days_made_better:
            exercise_not_last_days_made_better[e] = min(exercise_not_last_days_made_better[e])
        return [(e, exercise_not_last_days_made_better[e]) for e in sorted(exercise_not_last_days_made_better.keys(), key=lambda k: exercise_not_last_days_made_better[k])]



    def rule2_recommendation(self, user_id, techniques_of_user, cs_threshold):
        in_unit = self.sub_rule2_recommendation(user_id, techniques_of_user, cs_threshold, in_tehcnique=True)
        in_unit = sorted(in_unit)
        # TODO  out_unit might need to be with first 'behavrioal unit' then 'imagination unit' then ... . And not simply (other units)
        out_unit = self.sub_rule2_recommendation(user_id, techniques_of_user, cs_threshold, in_tehcnique=False)
        out_unit = sorted(out_unit)
        merged = []
        i,j = 0, 0
        while i < len(in_unit) and j < len(out_unit):
            if self.get_CS(in_unit[i][1]) < self.get_CS(out_unit[j][1]):
                merged.append(in_unit[i][0])
                i += 1
            else:
                merged.append(out_unit[j][0])
                j += 1
        while i < len(in_unit):
            merged.append(in_unit[i][0])
            i += 1
        while j < len(out_unit):
            merged.append(out_unit[j][0])
            j += 1
        return merged

    def sub_rule2_recommendation(self, user_id, techniques_of_user, cs_threshold, in_tehcnique = True):
        in_or_out = "IN" if in_tehcnique else "NOT IN"
        query = f"""
            SELECT Exercise.actionId, sudsQ1, vasQ1, fatigueQ1, sudsQ2, vasQ2, fatigueQ2
            FROM Session JOIN  list_exercises_ids ON Session.sessionId = list_exercises_ids.sessionId
            JOIN Exercise ON list_exercises_ids.list_exercises_ids = Exercise.exerciseId
            JOIN actions ON Exercise.actionId = actions.'מספר הפעולה'
            WHERE Session.userId = {user_id} AND actions.'מספר טכניקה' {in_or_out} ({','.join([str(t) for t in techniques_of_user])})
        """
        potential_good_exercises = []
        exercises_to_score = {}
        self.conn.cur.execute(query)
        all_exercises = self.conn.cur.fetchall()
        for exercise, suds1, vas1, fat1, suds2, vas2, fat2 in all_exercises:
            if exercise not in exercises_to_score:
                exercises_to_score[exercise] = []
            least_help = max(suds2 - suds1, vas2 - vas1, fat2 - fat1)
            worse_score = max(suds2, vas2, fat2)
            helped_somewhere = min(suds2 - suds1, vas2 - vas1, fat2 - fat1)
            if least_help <= 0 and helped_somewhere < 0 and self.get_CS(worse_score) == cs_threshold:
                potential_good_exercises.append(exercise)
            exercises_to_score[exercise].append((least_help, worse_score))
        good_exercises = {}
        for exercise in potential_good_exercises:
            scores_delta = [e[0] for e in exercises_to_score[exercise]]
            if sum((np.array(scores_delta) < 0) / len(scores_delta)) >= 0.7:
                score = [e[1] for e in exercises_to_score[exercise]]
                score = np.array(score)
                good_exercises[exercise] = score[score==min(score)]

        # TODO maybe now only take the after from P1, and not everywhere.

        good_exercises_list = sorted(good_exercises.keys(), key=lambda k: (exercises_to_score[k], len(exercises_to_score[k])))
        return [(g, good_exercises[g][0]) for g in good_exercises_list]

    def rule1_recommendation(self, user_id, exercises, techniques_of_user):
        if 1 in techniques_of_user:
            return []

        query = f"""
            SELECT Exercise.actionId, sudsQ1, vasQ1, fatigueQ1, sudsQ2, vasQ2, fatigueQ2
            FROM Session JOIN  list_exercises_ids ON Session.sessionId = list_exercises_ids.sessionId
            JOIN Exercise ON list_exercises_ids.list_exercises_ids = Exercise.exerciseId
            JOIN actions ON Exercise.actionId = actions.'מספר הפעולה'
            WHERE Session.userId = {user_id} AND actions.'מספר טכניקה' = 1 
            """
        self.conn.cur.execute(query)
        breathing_techniques = self.conn.cur.fetchall()
        bt_to_diffs = {}
        for bt in breathing_techniques:
            if bt[0] not in bt_to_diffs:
                bt_to_diffs[bt[0]] = []
            bt_to_diffs[bt[0]].append(max(bt[4] - bt[1], bt[5] - bt[2], bt[6] - bt[3]))
        good_breathing_techniques = {}
        for bt, diffs in bt_to_diffs.items():
            diffs = np.array(diffs)
            if sum((diffs <= 0) / len(diffs)) > 0.9:
                good_breathing_techniques[bt] = (diffs.mean(), len(diffs))

        ordered = sorted(good_breathing_techniques.keys(), key=lambda k: (good_breathing_techniques[k][0], good_breathing_techniques[k][1]))

        return [(o,good_breathing_techniques[o][0]) for o in ordered]


    def get_CS(self, value):
        if value <= 3:
            return 1
        if value <= 6:
            return 2
        if value <= 10:
            return 3
        raise ValueError("value is not in the range of 0-10. Get ", value)