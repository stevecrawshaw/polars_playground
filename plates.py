# %%
def main():
    plate = input("Plate: ")
    if is_valid(plate):
        print("Valid")
    else:
        print("Invalid")


# %%
def findpos(teststr, minmax="min"):
    numpos = []
    for i, x in enumerate(teststr):
        if x.isnumeric():
            numpos.append(i)
    if minmax == "min":
        return min(numpos)
    elif minmax == "max":
        return max(numpos)
    else:
        return None


# %%
def is_valid(s):
    length_ok = 2 <= len(s) <= 6
    alnums_ok = s.isalnum()
    first_2_chars = s[0:2].isalpha()
    # num_min_ok = findpos(s, "min") > 2
    if length_ok and not s.isalpha():
        num_max_ok = (findpos(s, "max") + 1) == len(s)
        first_num_not_0 = s[findpos(s, "min")] != "0"
    else:
        num_max_ok = False
        first_num_not_0 = False
    if length_ok and alnums_ok and first_2_chars and num_max_ok and first_num_not_0:
        return True
    else:
        return {
            "length_ok": length_ok,
            "alnums_ok": alnums_ok,
            "first_2_chars": first_2_chars,
            "num_max_ok": num_max_ok,
            "first_num_not_o": first_num_not_0,
        }


# %%


if __name__ == "__main__":
    main()
# %%

for i, x in enumerate("CS50P2"):
    print(i, x)

# %%
s = "CS50P2"
if s[-1].isnumeric() and s[3:-2].isalpha():
    print("bad")

# %%
"CS50P2"[2:-1]


# %%
def test_str(s):
    ls = []
    for x in s[2:]:
        ls.append(x.isnumeric())
    return ls


# %%

if test_str("CS50P2")[-1]:
    if any(test_str("CS50P2")[2:-1]):
        print("bad")


# %%
test_str("CS50P2")[-1]

# %%
test_str(("CS50P2")[2:-1])
