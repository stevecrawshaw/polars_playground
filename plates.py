# %%
def main():
    plate = input("Plate: ")
    if is_valid(plate):
        print("Valid")
    else:
        print("Invalid")


# %%
def test_not_zero_first(s):
    for i in s:
        if i.isnumeric():
            return i != "0"
    return True


# %%
def is_valid(s):
    length_ok = 2 <= len(s) <= 6
    alnums_ok = s.isalnum()
    first_2_chars = s[0:2].isalpha()
    not_zero_first = test_not_zero_first(s)
    if length_ok and alnums_ok and first_2_chars and not_zero_first:
        return midnums(s)
    else:
        return False


# %%


# %%
def midnums(s):
    """ "
    test if the chars after position 2 don't have numbers to the left of letters
    """

    def make_tup_alpha(s):
        s_2 = s[2:]
        return tuple([int(x.isalpha()) for x in s_2])

    stringbool = make_tup_alpha(s)
    match stringbool:
        case (0, 0, 1, 0) | (0, 0, 0, 1) | (0, 1, 1, 1) | (0, 1, 1, 0):
            return False
        case (0, 0, 1) | (0, 1, 0) | (0, 1, 1):
            return False
        case (0, 1):
            return False
        case _:
            return True


# %%
# %%
if __name__ == "__main__":
    main()
# %%
# %%
