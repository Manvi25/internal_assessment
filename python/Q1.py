def camel_to_snake(text):
  return re.sub(r'(?aZ)(?=[A-Z]|[0-9]|\W)', r'_\1', text).lower()