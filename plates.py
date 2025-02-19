#%%
def main():


    plate = input("Plate: ")
    if is_valid(plate):
        print("Valid")
    else:
        print("Invalid")
#%%
def findpos(teststr, minmax = "min"):
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

#%%

#%%
def is_valid(s):
    length_ok = 2<= len(s) <= 6
    alnums_ok = s.isalnum()
    first_2_chars = s[0:2].isalpha()
    #num_min_ok = findpos(s, "min") > 2
    num_max_ok = findpos(s, "max") == len(s)
    first_num_not_0 = s[findpos(s, "min")] != "0"


    if s.isalnum() and  and s[:2].isalpha() and s[7].isalpha():
        return False

if __name__ == "__main__":
    main()


#%%


#%%




#%%