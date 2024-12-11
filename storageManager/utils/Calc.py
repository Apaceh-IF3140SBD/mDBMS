class Calc:
    @staticmethod
    def get_format_char(self, col_type):
        type_map = {
            "int": "i",
            "float": "f",
        }
        return type_map.get(col_type, "")