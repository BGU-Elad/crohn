"""
at least once?
at least three days or exactly X days?
do we check this each day - or every X days - or only after an event?
how about weekends?
"""
conditions_at_least_once_in_x_days = lambda data,x: sum(data[-x]) >= 1
conditions_at_least_once_in_3_days = sum(dummy_last_3_days) >= 1
conditions_at_least_once_in_4_days = sum(dummy_last_4_days) >= 1


"""
how do we define "should"?
how do i know where they should be?
"""
should_be_in_x_but_in_y = lambda x: dummy_get_y() > x
should_be_in_2_but_in_1 = current_stage==1 and should_be_at == 2
should_be_in_3_but_in_1 = current_stage==1 and should_be_at == 3
should_be_in_3_but_in_2 = current_stage==2 and should_be_at == 3
should_be_in_4_but_in_1 = current_stage==1 and should_be_at == 4
should_be_in_4_but_in_2 = current_stage==2 and should_be_at == 4
should_be_in_4_but_in_3 = current_stage==3 and should_be_at == 4



"""
how do i know what is the minium days?
where do i see what is the minimum exercise number and days?
"""
patient_finished_exercise_premature = time_spent(unit_x) < minimum_time_needed(unit_x)


"""
where do i see min exercise for p1?
where do i see what is finished?

what happens if finished exerciese for p1, but prematurely?
"""
patient_finished_min_exercise_p1 = state_of_p1 > min_excerise(p1)


"""
same questions as before
"""
finished_minimum_dats_unit4_but_not_exercise = days_unit4 >= minimum_days_unit4 and not finished_exercise_unit4


one_year = started_date + timedelta(days=365) < datetime.now() and t4_date + datetime(90) < datetime.now()



"""
at least two weeks after phase p1?
when do we check this?
"""
no_exercise_two_weeks_after_phase_p1 = days_since_p1 > 14 and finished_exercise_p1

"""
at least one weak ? every day since 1 week?
"""
no_exercise_one_week = days_since_last > 7 and finished_exercise_p1
no_exercise_two_week = days_since_last > 2*7
no_exercise_three_week = days_since_last > 3*7
no_exercise_four_week = days_since_last > 4*7


"""
do weekends count? is it a full cycle or sunday to saturaday?
"""
exercies_at_least_6_days_a_week = sum(exercise_days) >= 6


"""
at least x techniques or exactly x techniques?
"""
days_spent_3_unit_1_learned_1 = current_unit == 1 and days_spent>=3 and number_of_techniques_in_unit ==1
days_spent_5_unit_1_learned_2 = current_unit == 2 and days_spent>=5 and number_of_techniques_in_unit ==1
days_spent_3_unit_2_learned_1 = current_unit == 2 and days_spent>=3 and number_of_techniques_in_unit ==1
days_spent_6_unit_2_learned_2 = current_unit == 2 and days_spent>=6 and number_of_techniques_in_unit ==2
days_spent_3_unit_3_learned_1 = current_unit == 3 and days_spent>=3 and number_of_techniques_in_unit ==1
days_spent_6_unit_3_learned_2 = current_unit == 3 and days_spent>=6 and number_of_techniques_in_unit ==2


"""
what is the definition of deterioration
"""
deterioration_all_scales_4_days = all(deterations[-4:])==True


"""
what is this
"""
one_session_per_week_no_ended_with_after_scales = _



"""
what is the definition of stagnation
every day of the timeframe? 
"""
improve_or_stag = (suds[-1] > suds[-2] or fat[-1] > fat[-2] or vas[-1] > vas[-2]) or (stagnation)
improve = (suds[-1] > suds[-2] or fat[-1] > fat[-2] or vas[-1] > vas[-2])

"""
what is the feginition of deterioration
every time frame? at least once?
"""
deterioration = _


"""
dependant on a unit?
"""
exercise_atleast_80_percent_of_min_exercise_defined_as_mandatory = sum(exercise) >= 0.8*min_exercise


"""
what
"""
before_vs_after_tendency_for_3_month_show_improve_or_stagnation = _
before_vs_after_tendency_for_3_month_show_det = _

"""
before or after what?
"""
before_after_in_c1 = _
before_after_in_c2 = _
before_after_in_c3 = _
before_sc1_after_sc2 = _
before_sc3_after_sc2 = _
before_sc2_after_sc3 = _
before_sc2_after_sc1 = _


"""
what
"""
exercis_same_exercise__up_to_three_different_exercises = _

""""
in same unit? phase? different exercieses in what?
"""
exercise_at_leat_4_different_exercieses = _

"""
meaning we are in the same sc group?
change what - in the improvment/stagnation?
change between when?
"""
change_within_sc_group = _
before_sc1_after_sc3 = _
change_from_sc2_to_sc3_or_sc1_to_sc2_or_sc3 = before_sc2_after_sc3 or before_sc1_after_sc2 or before_sc1_after_sc3

"""
what
"""
stagnation_in_before_and_after_tendency_across_1_last_month = _
improvement_in_before_and_after_tendency_across_1_last_month = _
deterioration_in_before_and_after_tendency_across_1_last_month = _


"""
before and after what?
"""
cs_stagnation_2_3 = before==after and after in [2,3]

"""
what
"""
improvement_in_all_cs_withing_and_between = _


rule_1 = conditions_at_least_once_in_3_days and improve_or_stag and change_within_sc_group
rule_2 = conditions_at_least_once_in_3_days and improve_or_stag and before_sc3_after_sc2
rule_3 = conditions_at_least_once_in_4_days and improve and before_sc2_after_sc1
rule_4 = conditions_at_least_once_in_3_days and deterioration and change_within_sc_group
rule_5 = deterioration and change_from_sc2_to_sc3_or_sc1_to_sc2_or_sc3
rule6 = should_be_in_2_but_in_1 and improve_or_stag
rule7 = should_be_in_2_but_in_1 and deterioration
rule8 = should_be_in_3_but_in_1 and improve_or_stag
rule9 = should_be_in_3_but_in_1 and deterioration
rule10 = should_be_in_3_but_in_2 and improve_or_stag
rule11 = should_be_in_3_but_in_2 and deterioration
rule12 = should_be_in_4_but_in_1 and improve_or_stag
rule13 = should_be_in_4_but_in_1 and deterioration
rule14 = should_be_in_4_but_in_2 and improve_or_stag
rule15 = should_be_in_4_but_in_2 and deterioration
rule16 = should_be_in_4_but_in_3 and improve_or_stag
rule17 = should_be_in_4_but_in_3 and deterioration
rule18 = finished_minimum_dats_unit4_but_not_exercise
rule19 = patient_finished_min_exercise_p1
rule20 = finished_minimum_dats_unit4_but_not_exercise and exercise_atleast_80_percent_of_min_exercise_defined_as_mandatory
rule21 = one_year
rule22 = no_exercise_two_weeks_after_phase_p1 and before_vs_after_tendency_for_3_month_show_improve_or_stagnation
rule23 = no_exercise_two_weeks_after_phase_p1 and before_vs_after_tendency_for_3_month_show_det
rule24 = no_exercise_one_week and improve
rule25 = improve_or_stag and before_after_in_c1
rule26 = improve_or_stag and before_after_in_c2
rule27 = improve_or_stag and before_after_in_c3
rule28 = improve_or_stag and before_sc3_after_sc2
rule29 = improve_or_stag and before_sc2_after_sc1
rule30 = deterioration and before_after_in_c1
rule31 = deterioration and before_after_in_c2
rule32 = deterioration and before_after_in_c3
rule33 = deterioration and before_sc3_after_sc2
rule34 = deterioration and before_sc1_after_sc2
rule35 = no_exercise_two_week
rule36 = no_exercise_three_week
rule37 = no_exercise_four_week
rule38 = exercies_at_least_6_days_a_week and exercis_same_exercise__up_to_three_different_exercises and stagnation_in_before_and_after_tendency_across_1_last_month and before_after_in_c1
rule39 = exercies_at_least_6_days_a_week and exercis_same_exercise__up_to_three_different_exercises and stagnation_in_before_and_after_tendency_across_1_last_month and cs_stagnation_2_3
rule40 = exercis_same_exercise__up_to_three_different_exercises and improvement_in_before_and_after_tendency_across_1_last_month and improvement_in_all_cs_withing_and_between
rule41 = exercis_same_exercise__up_to_three_different_exercises and deterioration_in_before_and_after_tendency_across_1_last_month
rule42 = exercies_at_least_6_days_a_week and exercise_at_leat_4_different_exercieses
rule43 = days_spent_3_unit_3_learned_1
rule44 = days_spent_5_unit_1_learned_2
rule45 = days_spent_3_unit_2_learned_1
rule46 = days_spent_6_unit_2_learned_2
rule47 = days_spent_3_unit_3_learned_1
rule48 = days_spent_6_unit_3_learned_2
rule49 = days_spent_3_unit_3_learned_1
rule50 = days_spent_6_unit_3_learned_2
rule51 = deterioration_all_scales_4_days and deterioration and change_from_sc2_to_sc3_or_sc1_to_sc2_or_sc3
rule52 = one_session_per_week_no_ended_with_after_scales









"""
patient can be in unit 1 but should be in unit 3 with improvments, AND finished unit x premature
"""