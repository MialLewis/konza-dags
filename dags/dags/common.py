def get_gender_code(firstname):
    return "M" if abs(hash(firstname)) % 2 else "F"

def get_dob(firstname, lastname):
    name = f"{firstname} {lastname}"
    
    dd = abs(hash(name)) % 28
    mm = abs(hash(name)) % 12
    yyyy = abs(hash(name)) % 83 + 1940  

    return "%04d%02d%02d" % (yyyy, mm, dd)

