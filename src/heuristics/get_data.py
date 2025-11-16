import json
import random
from datetime import datetime, timedelta

import numpy as np
from scipy.stats import linregress

from src.heuristics.orderings import score_pair_to_CS, technique_order, score_to_CS
from src.heuristics.queries import *
from src.utils.constants import FATIGUE, SUDS, VAS, DATE_FORMAT, BIG_DAYS, STAG, DET, MISC, INC, MINUS_TIME, MALE, \
    WITHIN, BETWEEN

from src.utils.utils import ExerciseData, calc_relative_delta, get_now


def get_users(conn):
    query = USER_LEVEL_QUERY
    conn.cur.execute(query)
    users = conn.cur.fetchall()
    return users


def get_users_time(conn):
    time_format = "%H:%M"
    query = USER_TIME_QUERY
    conn.cur.execute(query)
    users = conn.cur.fetchall()
    user_time_map = {}
    for user in users:
        user = list(user)
        if user[1] is None:
            user[1] = "08:00"
        if user[2] is None:
            user[2] = "20:00"
        morn = datetime.strptime(user[1], time_format)
        even = datetime.strptime(user[2], time_format)
        user_time_map[user[0]] = ((morn + (even - morn) / 2).time()).strftime(time_format)
    return user_time_map


def get_level_of_current_exercise(conn, user_id: int, days: int = 14, minus_time = MINUS_TIME):
    query = LEVEL_OF_CURRENT_EXERCISE_QUERY.format(user_id=user_id, days=days, minus_time=minus_time)
    conn.cur.execute(query)
    levels = conn.cur.fetchall()
    return [level[0] for level in levels]


def calculate_scores_and_deltas(scores1, scores2):
    scores = [score_pair_to_CS(scores1[i], scores2[i]) for i in range(len(scores1))]
    relative_deltas = [calc_relative_delta(scores1[i], scores2[i]) for i in range(len(scores1))]
    relative_deltas = np.array(relative_deltas)
    best_index = np.argmin(scores)
    best_score = scores[best_index]
    return scores, relative_deltas, best_index, best_score


def get_exercise_of_user(conn, user_id: int, time: int, user_hour: str, min_percent: float = 0.8, minus_time=MINUS_TIME):
    levels = get_level_of_current_exercise(conn, user_id, days=365*5,minus_time=minus_time)
    time_direction = "<" if time == 0 else ">"
    query = EXERCISES_OF_USER_QUERY.format(user_id=user_id, time_direction=time_direction, user_hour=user_hour,
                                           min_percent=min_percent)

    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    exercises = [e for e in exercises]
    exercises_dict = {}
    for exercise in exercises:
        suds1 = json.loads(exercise[2])
        suds2 = json.loads(exercise[3])
        fatigue1 = json.loads(exercise[4])
        fatigue2 = json.loads(exercise[5])
        vas1 = json.loads(exercise[6])
        vas2 = json.loads(exercise[7])

        suds_scores, relative_deltas_suds, suds_max_index, max_suds = calculate_scores_and_deltas(suds1, suds2)
        fat_scores, relative_deltas_fat, fat_max_index, max_fatigue = calculate_scores_and_deltas(fatigue1, fatigue2)
        vas_scores, relative_deltas_vas, vas_max_index, max_vas = calculate_scores_and_deltas(vas1, vas2)

        metrics = [max_suds, max_fatigue, max_vas]
        metrics_arg_max = [suds_max_index, fat_max_index, vas_max_index]
        relative_deltas = [relative_deltas_suds, relative_deltas_fat, relative_deltas_vas]
        scores = [suds_scores, fat_scores, vas_scores]

        best_change_index = np.argmin(metrics)
        best_change = metrics[best_change_index]

        relative_delta = relative_deltas[best_change_index][metrics_arg_max[best_change_index]]
        score = scores[best_change_index][metrics_arg_max[best_change_index]]

        relative_delta_count = len(
            relative_deltas[best_change_index][relative_deltas[best_change_index] == relative_delta])

        score_count = sum(np.array(scores[best_change_index]) == score)

        exercises_dict[exercise[0]] = ExerciseData(
            exercise=exercise[0],
            technique=technique_order(exercise[1]),
            change=best_change,
            relative_delta=relative_delta,
            score_count=score_count,
            count=relative_delta_count,
            in_out_unit=exercise[0] in levels
        )
    return exercises_dict


def get_exercise_for_metric_of_user(conn, user_id: int, metric: int, metric_percent: int = 0.80,
                                    other_percent: int = 0.40, minus_time=MINUS_TIME):
    levels = get_level_of_current_exercise(conn, user_id, minus_time=minus_time)
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

    query = EXERCISES_OF_METRIC_FOR_USER_QUERY.format(user_id=user_id, before=before, after=after,
                                                      suds_percent=suds_percent,
                                                      fatigue_percent=fatigue_percent, vas_percent=vas_percent)

    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    exercises = [e for e in exercises]
    exercises_dict = {}
    for exercise in exercises:
        met1 = json.loads(exercise[2])
        met2 = json.loads(exercise[3])

        met_scores, relative_deltas_met, met_max_index, max_met = calculate_scores_and_deltas(met1, met2)

        relative_delta = relative_deltas_met[met_max_index]
        relative_delta_count = len(relative_deltas_met[relative_deltas_met == relative_delta])
        exercises_dict[exercise[0]] = ExerciseData(
            exercise=exercise[0],
            change=max_met,
            relative_delta=relative_delta,
            count=relative_delta_count,
            in_out_unit=exercise[0] in levels
        )
    return exercises_dict


def get_forgotten_exercise_of_user(conn, user_id: int, days: int = 30, min_percent: float = 0.8, minus_time=MINUS_TIME):
    query = FORGOTTEN_EXERCISES_OF_USER_QUERY.format(user_id=user_id, days=days, min_percent=min_percent, minus_time=minus_time)
    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    exercises = [e for e in exercises]
    exercises_dict = {}
    for exercise in exercises:
        exercises_dict[exercise[0]] = {
            "exercise": exercise[0],
            "count": exercise[1]
        }
    return exercises_dict


def get_fourth_carousal_exercise_of_user(conn, user_id, days=60, deteriorate=0.5, minus_time=MINUS_TIME):
    query = FOURTH_CARUSAL_EXERCISE_OF_USER_QUERY.format(user_id=user_id, days=days, deterior=deteriorate, minus_time=minus_time)
    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    exercises = [e for e in exercises]
    exercises_dict = {}
    for exercise in exercises:
        exercises_dict[exercise[0]] = {
            "exercise": exercise[0],
        }
    return exercises_dict


def id_to_message(conn, user_id, m_or_f):
    query = ID_TO_MESSAGE_QUERY.format(id=user_id)
    conn.cur.execute(query)
    message = conn.cur.fetchone()
    if message is None:
        return ""
    if m_or_f.lower() == MALE.lower():
        message = message[0:3]
    else:
        message = message[3:]

    message = random.choice(message)
    return message


def get_user_sex(conn, user):
    query = USER_SEX_QUERY.format(user=user)
    conn.cur.execute(query)
    sex = conn.cur.fetchone()
    return sex[0] if type(sex) is tuple else sex


def get_should_be_unit(conn, user, minus_time = MINUS_TIME):
    # query = SHOULD_BE_UNIT_QUERY.format(user=user)
    # conn.cur.execute(query)
    # start_date = conn.cur.fetchone()
    query = CURRENT_UNIT_QUERY.format(user=user)
    conn.cur.execute(query)
    current_unit = conn.cur.fetchone()
    current_unit = current_unit[0]

    query = CURRENT_UNIT_TIME_QUERY.format(user=user)
    conn.cur.execute(query)
    start_date = conn.cur.fetchone()


    if start_date is None:
        return 0
    query = LEVEL_DAYS_QUERY
    conn.cur.execute(query)
    level_days = conn.cur.fetchall()
    now_time = get_now(minus_time)
    cleaned = start_date[0].split('.')[0]
    start_date = datetime.strptime(cleaned, DATE_FORMAT)
    level = -1
    for level, days in level_days:
        if level < current_unit:
            continue
        if start_date + timedelta(days=days) < now_time:
            start_date = start_date + timedelta(days=days)
        else:
            break
    return level
    # return should_be_unit[0] if type(should_be_unit) == tuple else should_be_unit


def get_current_t(conn, user, minus_time = MINUS_TIME):
    query = CURRENT_T_QUERY.format(user=user)
    conn.cur.execute(query)
    current_t = conn.cur.fetchone()
    if current_t is None or any([t is None for t in current_t]):
        return 0, 0, [0, 0, 0, 0, 0]
    current_t = [t.split(" ")[0] for t in current_t] #because SOMEONE doesnt want to give me a DB and gives me an EXCEL!!!!!!!!!!!!!!!!!
    current_t = [datetime.strptime(t, DATE_FORMAT[:8]) for t in current_t]
    now = get_now(minus_time)
    t_i = -1
    t = now
    for t_i, t in enumerate(current_t):
        if t > now:
            break
    return t_i, (current_t[t_i-1] - now).days if t_i > 0 else None, [(t - now).days for t in current_t]


def get_current_unit(conn, user):
    query = CURRENT_UNIT_QUERY.format(user=user)
    conn.cur.execute(query)
    current_unit = conn.cur.fetchone()
    return current_unit[0] if type(current_unit) is tuple else current_unit


def get_exercise_of_unit(conn, unit):
    query = EXERCISE_OF_UNIT_EXERCISES_QUERY
    conn.cur.execute(query)
    actions = conn.cur.fetchall()
    actions = [a[0] for a in actions if int(a[1].split(".")[0]) == unit]
    return actions


def get_n_exercises(conn, user, unit):
    actions = get_exercise_of_unit(conn, unit)
    query = N_EXERCISES_QUERY.format(actions=','.join(str(a) for a in actions), user=user)
    conn.cur.execute(query)
    n_exercises = conn.cur.fetchone()
    return n_exercises[0] if type(n_exercises) is tuple else n_exercises


def get_time_since_starting_unit(conn, user, unit, minus_time = MINUS_TIME):
    query = TIME_SINCE_START_UNIT_QUERY.format(user=user, unit=unit)

    conn.cur.execute(query)
    start_date = conn.cur.fetchone()
    if start_date is None:
        query = START_OF_UNIT_QUERY.format(user=user, unit=unit)
        conn.cur.execute(query)
        start_date = conn.cur.fetchone()
    if start_date is None:
        return -1, None
    cleaned = start_date[0].split('.')[0]
    start_date = datetime.strptime(cleaned, DATE_FORMAT)
    now = get_now(minus_time)
    return (now - start_date).days, start_date


def get_last_exercise_date(conn, user):
    query = LAST_EXERCISE_DATE_QUERY.format(user=user)
    conn.cur.execute(query)
    last_exercise_time = conn.cur.fetchone()

    if last_exercise_time is None:
        return None
    cleaned = last_exercise_time[0].split('.')[0]
    last_exercise_time = datetime.strptime(cleaned, DATE_FORMAT)
    return last_exercise_time


def get_percentage_done_of_unit(conn, unit, n_exercises):
    actions = get_exercise_of_unit(conn, unit)
    return n_exercises / len(actions) if len(actions) > 0 else 0, len(actions)


def number_of_days_in_unit(conn, user, unit, minus_time = MINUS_TIME):
    actions = get_exercise_of_unit(conn, unit)
    query = NUMBER_OF_DAYS_FOR_UNIT_QUERY.format(actions=','.join([str(a) for a in actions]), user=user)
    conn.cur.execute(query)
    last_exercise_time = conn.cur.fetchone()
    if last_exercise_time is None:
        query = START_OF_UNIT_QUERY.format(user=user, unit=unit)
        conn.cur.execute(query)
        last_exercise_time = conn.cur.fetchone()
    if last_exercise_time is None:
        return -1
    cleaned = last_exercise_time[0].split('.')[0]
    last_exercise_time = datetime.strptime(cleaned, DATE_FORMAT)
    now = get_now(minus_time)
    return (now - last_exercise_time).days


def get_min_n_exercises_for_unit(conn, unit):
    actions = get_exercise_of_unit(conn, unit)
    query = MIN_N_EXERCISES_FOR_UNIT_QUERY.format(actions=','.join([str(a) for a in actions]))
    conn.cur.execute(query)
    done_exercises = conn.cur.fetchone()
    return done_exercises[0]


def get_min_n_days_for_unit(conn, unit):
    query = MIN_N_DAYS_FOR_UNIT_QUERY.format(unit=unit)
    conn.cur.execute(query)
    done_exercises = conn.cur.fetchone()
    return done_exercises[0]


def get_x_sessions_back_with_y_session_where_after_is_higher_than_before_in_z_scales(conn, user, x=4, y=2, z=2):
    query = X_SESSIONS_BACK_WITH_Y_SESSIONS_WHERE_AFTER_IS_HIGHER_THAN_BEFORE_IN_Z_SCALES_QUERY.format(user=user,
                                                                                                       x=x, y=y, z=z)
    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    exercises = [list(e) for e in exercises if sum(e) >= z]
    return len(exercises) >= y

    # exercises = [list(e) for e in exercises]
    # exercises = np.array(exercises)
    # exercises = np.sum(exercises, axis=0)
    # return len(exercises[exercises >= z]) >= y


def get_n_sessions_per_x_days_that_do_not_have_an_after_scales_and_done_session(conn, user, days, minus_time = MINUS_TIME):
    query = N_SESSIONS_PER_X_DAYS_THAT_DO_NOT_HAVE_AN_AFTER_AND_DONE_SESSION.format(user=user, days=days, minus_time=minus_time)
    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    return sum([e[0] for e in exercises]) > 0


def get_n_different_exercises_per_x_samples(conn, user, samples):
    query = GET_N_DIFFERENT_EXERCISES_PER_X_SAMPLES.format(user=user, samples=samples)
    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    return exercises[0][0]


def get_n_exercises_in_past_x_days(conn, user, days, minus_time = MINUS_TIME):
    query = GET_N_EXERCISES_IN_PAST_X_DAYS.format(user=user, days=days, minus_time=minus_time)
    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    return exercises[0][0]


def get_trend(conn, user, measurements=14):
    query = TRENDS_QUERY.format(user=user, measurements=measurements)
    conn.cur.execute(query)
    exercises = conn.cur.fetchall()
    exercises = [list(e) for e in exercises]
    exercises = np.array(exercises)

    if len(exercises) < 2:
        # print("not enough exercises")
        return MISC, WITHIN, '0-0'

    suds_q1_regression = np.array([e[0] for e in exercises])
    fatigue_q1_regression = np.array([e[1] for e in exercises])
    vas_q1_regression = np.array([e[2] for e in exercises])
    suds_q2_regression = np.array([e[3] for e in exercises])
    fatigue_q2_regression = np.array([e[4] for e in exercises])
    vas_q2_regression = np.array([e[5] for e in exercises])

    def calculate_linear_regress(regression):
        return linregress(
            list(range(len(regression))), regression

            # [0, len(regression)], [regression[0], regression.mean()]
        )

    suds_q1slope, suds_q1intercept, suds_q1r_value, suds_q1p_value, suds_q1std_err = \
        calculate_linear_regress(suds_q1_regression)
    fatigue_q1slope, fatigue_q1intercept, fatigue_q1r_value, fatigue_q1p_value, fatigue_q1std_err = \
        calculate_linear_regress(fatigue_q1_regression)
    vas_q1slope, vas_q1intercept, vas_q1r_value, vas_q1p_value, vas_q1std_err = \
        calculate_linear_regress(vas_q1_regression)
    suds_q2slope, suds_q2intercept, suds_q2r_value, suds_q2p_value, suds_q2std_err = \
        calculate_linear_regress(suds_q2_regression)
    fatigue_q2slope, fatigue_q2intercept, fatigue_q2r_value, fatigue_q2p_value, fatigue_q2std_err = \
        calculate_linear_regress(fatigue_q2_regression)
    vas_q2slope, vas_q2intercept, vas_q2r_value, vas_q2p_value, vas_q2std_err = \
        calculate_linear_regress(vas_q2_regression)

    # use plotly to plot the regression line
    # import plotly.graph_objects as go
    # fig = go.Figure()
    # fig.add_trace(
    # go.Scatter(x=list(range(len(suds_q1_regression))), y=suds_q1_regression, mode='markers', name='suds_q1')
    # )
    # fig.add_trace(
    # go.Scatter(x=list(range(len(suds_q1_regression))),
    # y=suds_q1slope*np.array(list(range(len(suds_q1_regression))))+suds_q1intercept, mode='lines', name='suds_q1')
    # )
    # fig.show()

    def calculate_trend(q1slope, q2slope, q1intercept, q2intercept, q1_regression, q2_regression):
        if q1slope <= 0 and q2slope <= 0:
            q1_reg = q1slope * (len(q1_regression) - 1) + q1intercept
            q2_reg = q2slope * (len(q2_regression) - 1) + q2intercept
            q1_reg = q1_regression[-1]
            q2_reg = q2_regression[-1]
            if q1_reg - q2_reg > 1:
                curr_trend = INC
            elif 1 >= q1_reg - q2_reg >= 0:
                curr_trend = STAG
            else:
                curr_trend = MISC
        else:
            curr_trend = DET
        before_within = score_to_CS(q1_regression[0]) - score_to_CS(q1_regression[-1])
        before = f"{score_to_CS(q1_regression[0])}-{score_to_CS(q1_regression[-1])}"
        after_within = score_to_CS(q2_regression[0]) - score_to_CS(q2_regression[-1])
        after = f"{score_to_CS(q2_regression[0])}-{score_to_CS(q2_regression[-1])}"
        return curr_trend, before_within, before, after_within, after

    suds_trend, before_suds_within, before_suds, after_suds_within, after_suds = calculate_trend(
        suds_q1slope, suds_q2slope, suds_q1intercept, suds_q2intercept, suds_q1_regression, suds_q2_regression
    )

    fatigue_trend, before_fatigue_within, before_fat, after_fatigue_within, after_fat = calculate_trend(
        fatigue_q1slope, fatigue_q2slope, fatigue_q1intercept, fatigue_q2intercept, fatigue_q1_regression,
        fatigue_q2_regression
    )

    vas_trend, before_vas_within, before_vas, after_vas_within, after_vas = calculate_trend(
        vas_q1slope, vas_q2slope, vas_q1intercept, vas_q2intercept, vas_q1_regression, vas_q2_regression
    )

    trends_num = [before_vas_within, before_fatigue_within, before_suds_within, after_suds_within, after_fatigue_within,
                  after_vas_within]
    trends_string = [before_vas, before_fat, before_suds, after_suds, after_fat, after_vas]

    eval_trends = []
    for tred in [INC, STAG, DET]:
        eval_trend = evaluate_trend(tred, suds_trend, fatigue_trend, vas_trend, trends_num, trends_string,
                           before_fatigue_within, after_fatigue_within, before_suds_within, after_suds_within,
                           before_vas_within, after_vas_within)
        eval_trends.append(eval_trend)
    inc_eval_trend, stag_eval_trend, det_eval_trend = eval_trends

    if inc_eval_trend[1] is not None:
        result = inc_eval_trend
    elif stag_eval_trend[1] is not None:
        result = stag_eval_trend
    elif det_eval_trend[1] is not None:
        result = det_eval_trend
    else:
        result = (MISC, WITHIN, '0-0')

    if result:
        return result
    # return MISC, 0, '0-0'
    # exercises = np.sum(exercises, axis=0)
    # trend = np.argmax(exercises)
    # return trend, score_to_CS(exercises[0]), score_to_CS(exercises[1])


def evaluate_trend(
        trend_type,
        suds_trend,
        fatigue_trend,
        vas_trend,
        trends_num,
        trends_string,
        before_fatigue_within,
        after_fatigue_within,
        before_suds_within,
        after_suds_within,
        before_vas_within,
        after_vas_within,
):
    if trend_type in [suds_trend, fatigue_trend, vas_trend]:
        trend_nums_to_use = [] # [before_vas, before_fat, before_suds, after_suds, after_fat, after_vas]
        trend_strings_to_use = []  # [before_vas, before_fat, before_suds, after_suds, after_fat, after_vas]

        sum_trend = [True]
        if suds_trend == trend_type:
            sum_trend.append(before_suds_within == 0 and after_suds_within == 0)
            trend_nums_to_use += [trends_num[2], trends_num[3]]
            trend_strings_to_use += [trends_string[2], trends_string[3]]
        if fatigue_trend == trend_type:
            sum_trend.append(before_fatigue_within == 0 and after_fatigue_within == 0)
            trend_nums_to_use += [trends_num[1], trends_num[4]]
            trend_strings_to_use += [trends_string[1], trends_string[4]]
        if vas_trend == trend_type:
            sum_trend.append(before_vas_within == 0 and after_vas_within == 0)
            trend_nums_to_use += [trends_num[0], trends_num[-1]]
            trend_strings_to_use += [trends_string[0], trends_string[-1]]
        index = np.argmax(trend_nums_to_use) if trend_type != DET else np.argmin(trend_nums_to_use)
        return trend_type, WITHIN if all(sum_trend) else BETWEEN, trend_strings_to_use[index]
    return None, None, None


def get_last_time_message(conn, user, e, minus_time = MINUS_TIME):
    if e == -1:
        query = LAST_MESSAGE_QUERY.format(user=user)
    else:
        query = LAST_TIME_MESSAGE_QUERY.format(user=user, e=e)
    conn.cur.execute(query)
    last_time = conn.cur.fetchone()
    if last_time is None:
        return BIG_DAYS
    last_time = datetime.strptime(last_time[0], DATE_FORMAT)
    return (get_now(minus_time) - last_time).days+1


def is_real_user(conn, user):
    query = IS_REAL_USER_QUERY.format(user=user)
    conn.cur.execute(query)
    real_user = conn.cur.fetchone()
    if real_user is None:
        return False
    return real_user[0] > 0
