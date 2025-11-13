def technique_order(technique):
    # if technique == 1:
    if technique <= 3:
        return 1
    return 2
    # if technique in [2, 3]:
    #     return 2
    # return 3


def score_to_CS(score):
    if score < 4:
        return 1
    if score < 8:
        return 2
    return 3


def score_pair_to_CS(score1, score2):
    score1 = score_to_CS(score1)
    score2 = score_to_CS(score2)
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
    return 7
