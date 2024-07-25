class KeyValueQueryParameter:
    key: str
    value: str

    def __init__(self, raw_input: str):
        if len([c for c in raw_input if c == '=']) != 1:
            raise ValueError(
                f'key-value query parameter must have value of form "{{key}}={{value}}" (got "{raw_input}")')

        self.key, self.value = raw_input.split('=', maxsplit=1)

    def __lt__(self, other):
        return self.key < other.key
