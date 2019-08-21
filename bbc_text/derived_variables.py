import pyspark.sql.functions as F


def _collapse_spaces(column):
    """
    Trim leading and trailing space, and collapse any other whitespace
    into a single space.
    """
    trimmed = F.trim(column)
    collapsed = F.regexp_replace(trimmed, '\s+', ' ')
    return collapsed

def word_count(df, text_column):
    """
    Takes a DataFrame and the name of a text column and counts the
    words (anything separated by a space).
    """
    cleaned = _collapse_spaces(df[text_column])
    splitted = F.split(cleaned, pattern='\s+')
    count = F.size(splitted)
    return count