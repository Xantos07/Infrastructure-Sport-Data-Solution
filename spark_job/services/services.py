import unicodedata

def normalize_column_name(name):
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.replace(' ', '_').replace("'", '_').replace('é', 'e').replace('è', 'e')
    return name.lower()