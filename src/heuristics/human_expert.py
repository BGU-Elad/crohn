from datetime import datetime, timedelta
import json
from typing import Optional

from src.heuristics.queries import EXERCISES_IN_PROGRESS_QUERY, USER_LEVEL_QUERY, NON_IMPROVED_QUERY, USER_WHERE, \
    CURRENT_PROGRESS_QUERY, USER_TO_FINISHED_EXERCISES, ACTIONS_NOT_DONE_QUERY, AVAILABLE_ACTIONS_QUERY, \
    TRAINING_DATA_QUERY
from src.utils.session_object import Session
import numpy as np

FREE_USER_LEVEL = 5
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

AT_LEAST_TWICE_A_DAY_MESSAGE = "try doing at least 2 times a day consecutively"
NO_RECOMMENDATION_TEXT = "no recommendations"
NO_TEXT = ""
NO_RECOMMENDATION = []


class HumanExpert:
    action_technique_name_index = 2
    action_serial_number_index = 4

    def __init__(self, conn):
        self.conn = conn

    def recommend(self):
        # level = self.get_level()
        # level_per_user = {user_id: level for user_id, level in level}

        improvement = self.get_improvement()
        improvement_per_user = {}
        for user, count, end_date, sessions, sessions_type, actions in improvement:
            improvement_per_user[user] = {
                'count': count,
                'end_date': end_date,
                'sessions': json.loads(sessions),
                'sessions_type': json.loads(sessions_type),
                'actions': json.loads(actions)
            }
            user_sessions = self.date_format(improvement_per_user[user]['sessions'],
                                             improvement_per_user[user]['sessions_type'])
            improvement_per_user[user]['user_sessions'] = user_sessions

        best_avg = self.get_best_average_level()

        different_technique = self.get_other_topic_exercise()
        different_technique = {
            user: [v[HumanExpert.action_serial_number_index] for v in values]
            for user, values in different_technique.items()
        }

        user_recommendations = {}
        for user_id, improvement in improvement_per_user.items():
            is_legal, consecutive_days, last_activity = self.check_legality(user_id, improvement, user_recommendations)
            improvement["count"] = len(improvement["user_sessions"])
            # if not is_legal: continue

            if improvement["count"] < 7:
                if last_activity is None or last_activity>7:
                    user_recommendations[user_id] = ([], AT_LEAST_TWICE_A_DAY_MESSAGE)
                    continue
                else:
                    user_recommendations[user_id] = ([], NO_RECOMMENDATION_TEXT)
                    continue

            if improvement["count"] == 7 or consecutive_days == 7:
                if not is_legal:
                    user_recommendations[user_id] = ([], AT_LEAST_TWICE_A_DAY_MESSAGE)
                elif len(improvement["actions"]) <= 1:
                    user_recommendations[user_id] = (different_technique[user_id], NO_TEXT)
                else:
                    user_recommendations[user_id] = best_avg[user_id], ""
                continue
            if improvement["count"] == 10 or consecutive_days == 10:
                user_recommendations[user_id] = best_avg[user_id], ""
                continue
            if (improvement["count"] > 13 and (improvement["count"] - 13) % 3 == 0) or (
                    consecutive_days > 13 and (consecutive_days - 13) % 3 == 0):
                user_recommendations[user_id] = (different_technique[user_id], NO_TEXT)
                continue
        return user_recommendations

    @staticmethod
    def check_legality(user_id, improvement, user_recommendations):
        prev_session = None
        index = -1
        last_session = None
        for index, session in enumerate(reversed(improvement["user_sessions"])):
            if index ==0: last_session = (datetime.now().date() - session.date).days
            if session.count() < 2 or (prev_session is not None and session.date - prev_session.date > timedelta(days=1)):
                user_recommendations[user_id] = (NO_RECOMMENDATION, AT_LEAST_TWICE_A_DAY_MESSAGE)
                return False, index, last_session
            prev_session = session
        return True, index, last_session

    def get_doing(self, userid, date):
        doing_cur = self.conn.cur.execute(EXERCISES_IN_PROGRESS_QUERY.format(userid=userid, date=date))
        doing, sessions = [], []
        for d, s in doing_cur:
            doing += json.loads(d)
            sessions += json.loads(s)

        versions_to_dates = {}
        for d, s in zip(doing, sessions):
            if d not in versions_to_dates:
                versions_to_dates[d] = []
            versions_to_dates[d].append(s)
        return versions_to_dates

    def get_level(self):
        level_cur = self.conn.cur.execute(USER_LEVEL_QUERY)
        level = [level for level in level_cur]
        return level

    def get_improvement(self):
        """
        get how much time the user did not improve
        improved is defined as well-being after exercise is greater than before, and one of the other metrics is better
        (in this case less bad)
        to do this we first find the last improved session, and then we find all the sessions after that as non-improved
        :return per user the consecutive days of no improvement with extra data
        """
        sessions_cur = self.conn.cur.execute(NON_IMPROVED_QUERY)
        sessions = [session for session in sessions_cur]
        return sessions

    @staticmethod
    def date_format(sessions, sessions_type):
        sessions_as_datetime = [datetime.strptime(session, DATE_FORMAT) for session in sessions]
        date_session_map = {}
        for index, session in enumerate(sessions_as_datetime):
            session_date = session.date()
            if session_date not in date_session_map:
                date_session_map[session_date] = []
            date_session_map[session_date].append(index)

        sessions_aggrieved = []
        for date, sessions in date_session_map.items():
            session_types = {"morning": None, "evening": None}
            for session_id in sessions:
                session_type = sessions_type[session_id]
                session = sessions_as_datetime[session_id]
                session_types[session_type.lower()] = session
            sessions_aggrieved.append(Session(**session_types))
        sessions_aggrieved = list(sorted(sessions_aggrieved, key=lambda x: x.date))
        return sessions_aggrieved

    def get_other_topic_exercise(
            self,
            same_or_diff_topic: Optional[str] = "same",
            user_id: Optional[int] = None,
            level: Optional[int] = None
    ):
        """

        :param same_or_diff_topic: True means same topic, false means different topic, None means any
        :param user_id: user id to filter or None if not relevant
        :param level: None if not relevant, int to only take rows with the same level
        :return:
        """
        assert same_or_diff_topic in ["same", "diff", None], \
            f"same_or_diff_topic must be 'same' or 'diff'. Got {same_or_diff_topic}"

        where = "" if user_id is None else USER_WHERE.format(user_id=user_id)

        current_program_id = self.conn.cur.execute(CURRENT_PROGRESS_QUERY)
        user_to_current_position = {user: [int(c) for c in json.loads(cpi)] for (user, cpi) in current_program_id}

        user_to_done_actions = {}
        user_to_done_actions_query = self.conn.cur.execute(USER_TO_FINISHED_EXERCISES.format(where=where))
        for user, techniqueId, versions, action_ids in user_to_done_actions_query:
            user_to_done_actions[user] = (json.loads(versions), json.loads(action_ids), json.loads(techniqueId))

        user_can_do = {}
        for user, (done_versions, done_ids, technique_ids) in user_to_done_actions.items():
            done_ids = [str(i) for i in done_ids]
            non_done_actions = self.conn.cur.execute(ACTIONS_NOT_DONE_QUERY.format(already_done=",".join(done_ids)))
            can_do = [nda for nda in non_done_actions]
            # can_do = self.conn.cur.execute(AVAILABLE_ACTIONS_QUERY.format(
            #     action = ",".join(done_ids),
            #     technique = ",".join([str(nda[HumanExpert.action_technique_name_index]) for nda in can_do])
            # ))

            cds = []
            for cd in can_do:
                version = int(cd[4].split(".")[0])
                if same_or_diff_topic == "same":
                    if version in user_to_current_position[user]:
                        cds.append(cd)
                elif same_or_diff_topic == "diff":
                    if version < user_to_current_position[user]:
                        cds.append(cd)
                elif level is not None and version == level:
                    cds.append(cd)
            user_can_do[user] = cds

        return user_can_do

    def get_best_average_level(self, start_date=None, end_date=None):
        where = ["TRUE"]

        if start_date is not None:
            where.append(f"startSession >= '{start_date}'")
        if end_date is not None:
            where.append(f"endSession <= '{end_date}'")
        where = " AND " + " AND ".join(where) if len(where) > 0 else ""

        level_cur = self.conn.cur.execute(TRAINING_DATA_QUERY.format(free_user_level=FREE_USER_LEVEL, where=where))

        user_level_avg = {}
        for (user,
             level,
             session,
             sudsQ1,
             sudsQ2,
             fatigueQ1,
             fatigueQ2,
             vasQ1,
             vasQ2,
             well_beingQ1,
             well_beingQ2
             ) in level_cur:
            level = int(level.split(".")[0])
            fat = fatigueQ1 - fatigueQ2
            sud = sudsQ1 - sudsQ2
            vas = vasQ1 - vasQ2
            well_being = well_beingQ2 - well_beingQ1
            if user not in user_level_avg:
                user_level_avg[user] = {}
            if level not in user_level_avg[user]:
                user_level_avg[user][level] = []
            user_level_avg[user][level].append((fat, sud, vas, well_being))
        for user, levels in user_level_avg.items():
            for level, values in levels.items():
                user_level_avg[user][level] = np.array(values).mean(0)

        # get the best level for each user
        best_avg = user_level_avg
        best_levels = {}
        for user, levels in best_avg.items():
            best_val = 0
            best_level = 0
            for level, (suds, vas, fat, wb) in levels.items():
                score = [suds, vas, fat, wb]
                score = sum(score) / len(score)
                if score > best_val:
                    best_val = score
                    best_level = level

            best_levels[user] = [
                values[4] for values in
                self.get_other_topic_exercise(same_or_diff_topic=None, user_id=user, level=best_level)[user]
            ]
        return best_levels
