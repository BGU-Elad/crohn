from datetime import datetime
from typing import Optional

import pandas as pd

from src.heuristics.get_data import get_current_t, get_exercise_of_user, get_users, get_users_time, \
    get_forgotten_exercise_of_user, get_fourth_carousal_exercise_of_user, get_exercise_for_metric_of_user, \
    is_real_user, get_last_exercise_date, get_trend, get_should_be_unit, get_current_unit, \
    get_time_since_starting_unit, get_n_exercises, get_percentage_done_of_unit, get_min_n_exercises_for_unit, \
    get_min_n_days_for_unit, \
    number_of_days_in_unit, get_x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales, \
    get_n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session, \
    get_n_different_exercises_per_x_samples, get_n_exercises_in_past_x_days, get_last_time_message, get_user_sex, \
    id_to_message

from src.utils.constants import FREE_USER_LEVEL, MORNING, EVENING, SUDS, FATIGUE, VAS, INC, STAG, DET, MISC, WITHIN, \
    BETWEEN, MINUS_TIME, MALE
from src.utils.utils import get_first_or_empty, take_two_from_each_cycle, get_now


class HumanExpert:
    def __init__(self, conn, minus_time=MINUS_TIME):
        self.conn = conn
        self.minus_time = minus_time

    def first_carousal(self, time: int):
        users = get_users(self.conn)
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        user_time = get_users_time(self.conn)
        time = MORNING if time == 0 else EVENING

        exercise_for_user = {}
        for user in users:
            user_hour = user_time[user[0]]
            current_t, _, _ = get_current_t(self.conn, user[0], self.minus_time)
            if current_t >= 2:
                exercise_of_user = get_exercise_of_user(self.conn, user_id=user[0], time=time, user_hour=user_hour, minus_time=self.minus_time)
                exercise_of_user = {exercise: exercise_of_user[exercise].__dict__() for exercise in exercise_of_user}
            else:
                exercise_of_user = []

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
                sorted_exercises = take_two_from_each_cycle(new_groups)
            exercise_for_user[user[0]] = sorted_exercises
        return exercise_for_user

    def second_carousal(self, metric: int, metric_percent: int = 0.80, other_percent: int = 0.40):
        users = get_users(self.conn)
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        exercise_for_user = {}
        for user in users:
            current_t, _, _ = get_current_t(self.conn, user[0], self.minus_time)
            if current_t >= 2:
                exercise_of_user = get_exercise_for_metric_of_user(self.conn, user_id=user[0], metric=metric,
                                                                   metric_percent=metric_percent,
                                                                   other_percent=other_percent, minus_time=self.minus_time)
                exercise_of_user = {exercise: exercise_of_user[exercise].__dict__() for exercise in exercise_of_user}
            else:
                exercise_of_user = []

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

    def third_carousal(self, days: int = 30, min_percent: float = 0.8):
        users = get_users(self.conn)
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        exercise_for_user = {}
        for user in users:
            current_t, _, _ = get_current_t(self.conn, user[0], self.minus_time)
            if current_t >= 2:
                exercise_of_user = get_forgotten_exercise_of_user(self.conn, user_id=user[0], days=days,
                                                                  min_percent=min_percent, minus_time=self.minus_time)
            else:
                exercise_of_user = []

            df = pd.DataFrame(exercise_of_user).T
            sorted_exercises = []
            if len(df) > 0:
                df = df.sort_values(["count"])
                sorted_exercises = df["exercise"].tolist()
            exercise_for_user[user[0]] = sorted_exercises
        return exercise_for_user

    def fourth_carousal(self):
        users = get_users(self.conn)
        users = [user for user in users if int(user[1]) >= FREE_USER_LEVEL]
        exercise_for_user = {}
        for user in users:
            current_t, _, _ = get_current_t(self.conn, user[0], self.minus_time)
            if current_t >= 2:
                exercise_of_user = get_fourth_carousal_exercise_of_user(self.conn, user_id=user[0], minus_time=self.minus_time)
            else:
                exercise_of_user = []

            df = pd.DataFrame(exercise_of_user).T
            sorted_exercises = []
            if len(df) > 0:
                df = df.sort_values(["exercise"])
                sorted_exercises = df["exercise"].tolist()
            exercise_for_user[user[0]] = sorted_exercises
        return exercise_for_user

    def recommend(self, time: Optional[int] = 0):
        assert time in [0, 1], "time 0 is morning and time 1 is evening"
        first_carousal = self.first_carousal(MORNING)
        first2_carousal = self.first_carousal(EVENING)
        second1_carousal = self.second_carousal(SUDS)
        second2_carousal = self.second_carousal(FATIGUE)
        second3_carousal = self.second_carousal(VAS)
        third_carousal = self.third_carousal()
        fourth_carousal = self.fourth_carousal()
        messages, message_indexes, user_trends = self.get_messages()
        user_to_recommendation = {}
        all_users = set(first_carousal.keys()).union(second1_carousal.keys()).union(second2_carousal.keys()).union(
            second3_carousal.keys()).union(third_carousal.keys()).union(fourth_carousal.keys())
        for user in all_users:
            user_to_recommendation[user] = {
                "001A": first_carousal.get(user, []),
                "001B": first2_carousal.get(user, []),
                "002A": second1_carousal.get(user, []),
                "002B": second2_carousal.get(user, []),
                "002C": second3_carousal.get(user, []),
                "003": third_carousal.get(user, []),
                "004": fourth_carousal.get(user, []),
                "message": (
                    get_first_or_empty(messages.get(user, [""])),
                    get_first_or_empty(message_indexes.get(user, [-1]))
                ),
                "trend": user_trends.get(user, [-1,-1,-1])[0],
                "WITHIN": user_trends.get(user, [-1, -1, -1])[1],
                "difference": user_trends.get(user, [-1, -1, -1])[2],
            }
        return user_to_recommendation

    def get_messages(self):

        exercise_priority_message = [-1] + [63, 24, 25, 26, 27, 76] + list(range(12, 24)) + [66, 67, 68, 69, 70, 71, 72,
                                                                                             73, 74, 75] + list(
            range(1, 12)) + list(range(28, 66))
        exercise_message_interval = ({i: 2 for i in
                                      [63, 24, 25, 26, 27, 76] + list(range(12, 24)) + [66, 67, 68, 69, 70, 71, 72, 73,
                                                                                        74, 75] + list(
                                          range(1, 11))} | {8: 4, 9: 4} |
                                     {i: 2 for i in range(28, 47)}) | {47: 7, 48: 7, 49: 4 * 7} | {i: 6 for i in
                                                                                                   range(50, 66)} | {
                                        -1: 0} | {31: 1000, 32: 1000}

        user_to_message = {}
        user_to_trends = {}
        users_gender = {}
        users = get_users(self.conn)
        for user in users:
            user = user[0]
            real_user = is_real_user(self.conn, user)

            user_to_message[user] = []

            if not real_user:
                user_to_message[user].append(-1)
                continue
            user_sex = get_user_sex(self.conn, user)
            users_gender[user] = user_sex

            last_exercise_time = get_last_exercise_date(self.conn, user)
            if last_exercise_time is None:
                # print(user, "bad: no exercise")
                user_to_message[user].append(-1)
                continue

            trend, within_or_between, cs = get_trend(self.conn, user)
            user_to_trends[user] = (trend, within_or_between, cs)
            if trend == MISC:
                # print(user, "bad: no valid trend")
                user_to_message[user].append(-1)
                continue

            current_t, x_days_from_current_t, x_days_from_T_is = get_current_t(self.conn, user, self.minus_time)

            p_value = current_t if current_t < 5 else 4
            if p_value == 0:
                # print(user, "bad: no T value")
                user_to_message[user].append(-1)
                continue

            should_be_unit = get_should_be_unit(self.conn, user, self.minus_time)
            current_unit = get_current_unit(self.conn, user)
            n_exercises_from_current_unit = get_n_exercises(self.conn, user, current_unit)
            time_since_starting_unit, date_of_starting_unit = get_time_since_starting_unit(
                self.conn,
                user,
                current_unit,
                self.minus_time
            )
            percentage_done_of_unit = get_percentage_done_of_unit(
                self.conn,
                current_unit,
                n_exercises_from_current_unit
            )
            number_of_days_in_unit_value = number_of_days_in_unit(self.conn, user, current_unit, self.minus_time)
            min_n_exercises_for_unit = get_min_n_exercises_for_unit(self.conn, current_unit)
            min_n_days_for_unit = get_min_n_days_for_unit(self.conn, current_unit)
            x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales = \
                get_x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales(
                    self.conn, user, x=4, y=2, z=2
                )
            n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session = \
                get_n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session(
                    self.conn, user, current_t, self.minus_time
                )
            n_different_exercises_per_x_samples = get_n_different_exercises_per_x_samples(self.conn, user, 14)
            n_exercises_in_past_x_days = get_n_exercises_in_past_x_days(self.conn, user, 7, self.minus_time)

            if p_value == 1:
                if (get_now(self.minus_time) - last_exercise_time).days == 3 and (
                        trend in [INC, STAG]) and within_or_between == WITHIN:
                    user_to_message[user].append(1)
                if (get_now(self.minus_time) - last_exercise_time).days == 3 and (
                        trend in [INC, STAG]) and within_or_between == BETWEEN:
                    user_to_message[user].append(2)
                if (get_now(self.minus_time) - last_exercise_time).days == 3 and (trend in [DET]) and within_or_between == WITHIN:
                    user_to_message[user].append(10)
                if (get_now(self.minus_time) - last_exercise_time).days == 3 and (
                        trend in [DET]) and within_or_between == BETWEEN:
                    user_to_message[user].append(11)
                if 4 <= (get_now(self.minus_time) - last_exercise_time).days <= 14 and (trend in [INC]):
                    user_to_message[user].append(3)
                if 4 <= (get_now(self.minus_time) - last_exercise_time).days <= 14 and (trend in [STAG]):
                    user_to_message[user].append(4)
                if 4 <= (get_now(self.minus_time) - last_exercise_time).days <= 14 and (trend in [DET]):
                    user_to_message[user].append(5)
                if 15 <= (get_now(self.minus_time) - last_exercise_time).days <= 22:
                    user_to_message[user].append(6)
                if 23 <= (get_now(self.minus_time) - last_exercise_time).days <= 29:
                    user_to_message[user].append(7)
                if 30 <= (get_now(self.minus_time) - last_exercise_time).days <= 60:
                    user_to_message[user].append(8)
                if 60 <= (get_now(self.minus_time) - last_exercise_time).days <= 90:
                    user_to_message[user].append(9)

                if current_unit == 1 and number_of_days_in_unit_value >= 3 and n_exercises_from_current_unit <= 1:
                    user_to_message[user].append(66)
                if current_unit == 1 and number_of_days_in_unit_value >= 5 and n_exercises_from_current_unit <= 2:
                    user_to_message[user].append(67)
                if current_unit == 2 and number_of_days_in_unit_value >= 3 and n_exercises_from_current_unit <= 1:
                    user_to_message[user].append(68)
                if current_unit == 2 and number_of_days_in_unit_value >= 5 and n_exercises_from_current_unit <= 2:
                    user_to_message[user].append(69)
                if current_unit == 3 and number_of_days_in_unit_value >= 2 and n_exercises_from_current_unit <= 1:
                    user_to_message[user].append(70)
                if current_unit == 3 and number_of_days_in_unit_value >= 4 and n_exercises_from_current_unit <= 2:
                    user_to_message[user].append(71)
                if current_unit == 4 and number_of_days_in_unit_value >= 3 and n_exercises_from_current_unit <= 1:
                    user_to_message[user].append(72)
                if current_unit == 4 and number_of_days_in_unit_value >= 7 and n_exercises_from_current_unit <= 2:
                    user_to_message[user].append(73)
                if current_unit == 4 and number_of_days_in_unit_value >= 14 and n_exercises_from_current_unit <= 3:
                    user_to_message[user].append(74)
                if should_be_unit == 2 and current_unit == 1 and trend in [INC, STAG]:
                    user_to_message[user].append(12)
                if should_be_unit == 2 and current_unit == 2 and trend in [DET]:
                    user_to_message[user].append(13)
                if should_be_unit == 3 and current_unit == 1 and trend in [INC, STAG]:
                    user_to_message[user].append(14)
                if should_be_unit == 3 and current_unit == 1 and trend in [DET]:
                    user_to_message[user].append(15)
                if should_be_unit == 3 and current_unit == 2 and trend in [INC, STAG]:
                    user_to_message[user].append(16)
                if should_be_unit == 3 and current_unit == 2 and trend in [DET]:
                    user_to_message[user].append(17)
                if should_be_unit == 4 and current_unit == 1 and trend in [INC, STAG]:
                    user_to_message[user].append(18)
                if should_be_unit == 4 and current_unit == 1 and trend in [DET]:
                    user_to_message[user].append(19)
                if should_be_unit == 4 and current_unit == 2 and trend in [INC, STAG]:
                    user_to_message[user].append(20)
                if should_be_unit == 4 and current_unit == 2 and trend in [DET]:
                    user_to_message[user].append(21)
                if should_be_unit == 4 and current_unit == 3 and trend in [INC, STAG]:
                    user_to_message[user].append(22)
                if should_be_unit == 4 and current_unit == 3 and trend in [DET]:
                    user_to_message[user].append(23)
                if x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales:
                    user_to_message[user].append(76)
                if min_n_days_for_unit > number_of_days_in_unit_value and current_unit in [1, 2, 3]:
                    user_to_message[user].append(24)
                if min_n_days_for_unit > number_of_days_in_unit_value and current_unit in [4]:
                    user_to_message[user].append(25)
                if current_unit == 5:
                    user_to_message[user].append(26)
                if current_unit == 4 and percentage_done_of_unit < 0.8 * min_n_exercises_for_unit:
                    user_to_message[user].append(27)
            elif p_value > 1:
                if p_value == 2 and x_days_from_T_is[3 - 1] >= 6 * 30:
                    user_to_message[user].append(28)
                if p_value == 3 and x_days_from_T_is[4 - 1] >= 9 * 30:
                    user_to_message[user].append(29)
                if p_value == 4 and x_days_from_T_is[5 - 1] >= 12 * 30:
                    user_to_message[user].append(30)

                if p_value == 2 and current_unit == 5 and (
                        date_of_starting_unit - last_exercise_time).days > 7 and trend in [INC, STAG]:
                    user_to_message[user].append(31)
                if p_value == 2 and current_unit == 5 and (
                        date_of_starting_unit - last_exercise_time).days > 7 and trend in [DET]:
                    user_to_message[user].append(32)
                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days >= 3 and x_days_from_current_t == 3:
                    user_to_message[user].append(33)
                if p_value > 1 and 7 > (get_now(self.minus_time) - last_exercise_time).days >= 3:
                    user_to_message[user].append(34)

                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days == 7 and trend in [INC,
                                                                                                 STAG] and cs == "1-1":
                    user_to_message[user].append(35)
                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days == 7 and trend in [INC,
                                                                                                 STAG] and cs == "2-2":
                    user_to_message[user].append(36)
                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days == 7 and trend in [INC,
                                                                                                 STAG] and cs == "3-3":
                    user_to_message[user].append(37)
                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days == 7 and trend in [INC] and cs in ["3-1",
                                                                                                                 "3-2"]:
                    user_to_message[user].append(38)
                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days == 7 and trend in [INC] and cs in ["2-1"]:
                    user_to_message[user].append(39)
                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days == 7 and trend in [DET]:
                    if cs in ["1-1"]:
                        user_to_message[user].append(40)
                    if cs in ["2-2"]:
                        user_to_message[user].append(41)
                    if cs in ["3-3"]:
                        user_to_message[user].append(42)
                    if cs in ["2-3", "1-3"]:
                        user_to_message[user].append(43)
                    if cs in ["1-2"]:
                        user_to_message[user].append(44)

                if p_value > 1 and 21 >= (get_now(self.minus_time) - last_exercise_time).days >= 14:
                    user_to_message[user].append(45)
                if p_value > 1 and 28 >= (get_now(self.minus_time) - last_exercise_time).days >= 21:
                    user_to_message[user].append(46)
                if p_value > 1 and 8 * 7 >= (get_now(self.minus_time) - last_exercise_time).days >= 28:
                    user_to_message[user].append(47)
                if p_value > 1 and 12 * 7 >= (get_now(self.minus_time) - last_exercise_time).days >= 8 * 7:
                    user_to_message[user].append(48)
                if p_value > 1 and (get_now(self.minus_time) - last_exercise_time).days >= 12 * 7:
                    user_to_message[user].append(49)

                if p_value > 1 and n_different_exercises_per_x_samples <= 3:
                    if trend == STAG:
                        if n_exercises_in_past_x_days <= 3:
                            if cs == "1-1":
                                user_to_message[user].append(50)
                            if cs == "2-2" or cs == "3-3":
                                user_to_message[user].append(51)
                        else:
                            if cs == "1-1":
                                user_to_message[user].append(52)
                            if cs == "2-2" or cs == "3-3":
                                user_to_message[user].append(53)
                    if trend == INC:
                        if n_exercises_in_past_x_days <= 3:
                            if cs == "1-1":
                                user_to_message[user].append(54)
                            if cs == "2-2" or cs == "3-3":
                                user_to_message[user].append(55)
                            if cs == "3-1" or cs == "2-1":
                                user_to_message[user].append(56)
                            if cs == "3-2":
                                user_to_message[user].append(57)
                        else:
                            if cs == "1-1":
                                user_to_message[user].append(58)
                            if cs == "2-2" or cs == "3-3":
                                user_to_message[user].append(59)
                            if cs == "3-1" or cs == "2-1":
                                user_to_message[user].append(60)
                            if cs == "3-2":
                                user_to_message[user].append(61)
                    if trend == DET:
                        if n_exercises_in_past_x_days <= 3:
                            if cs[0] == cs[-1]:
                                user_to_message[user].append(62)
                            else:
                                user_to_message[user].append(63)
                        else:
                            if cs[0] == cs[-1]:
                                user_to_message[user].append(64)
                            else:
                                user_to_message[user].append(65)
                if n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session:
                    user_to_message[user].append(75)

        user_indexes = {}
        for user in user_to_message:
            exercises = []
            for e in user_to_message[user]:
                last = get_last_time_message(self.conn, user, e, self.minus_time)
                if last > exercise_message_interval[e]:
                    exercises.append(e)
            user_to_message[user] = exercises
            indexes = sorted(user_to_message[user], key=lambda x: exercise_priority_message.index(x))
            user_to_message[user] = [id_to_message(self.conn, id_, users_gender.get(user, MALE)) for id_ in indexes]
            user_indexes[user] = indexes
        return user_to_message, user_indexes, user_to_trends
