import re
import difflib

class SQLChecker:
    SQL_QUERY = [
        "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "JOIN", 
        "LEFT", "NATURAL", "GROUP", "LIMIT", "OFFSET", "VALUES", "CREATE", "TABLE", 
        "DISTINCT", "LIKE", "BETWEEN", "DROP", "ON"
    ]

    SQL_ORDER = ["BEGIN", "TRANSACTION", "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "COMMIT"]

    def __init__(self, query):
        query = query.upper()
        query = re.sub(r'([();])', r' \1 ', query)
        clear_query = query.replace(",", " ")
        self.query = re.findall(r'\S+', clear_query)  

    def check_syntax(self):
        if self.query[-1] != ";":
            return "Syntax Error: SQL query should end with a semicolon."
        
        if self.query.count('(') != self.query.count(')'):
            return "Error: Unbalanced parentheses in the query."

        first_word = self.query[0]
        if first_word not in ["BEGIN", "COMMIT", "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP"]:
            return f"Error: Unsupported SQL operation '{first_word}'"
        
        return None

    def check_typo(self):
        for word in self.query:
            if word.isalpha() and word not in self.SQL_QUERY:
                suggestions = difflib.get_close_matches(word, self.SQL_QUERY, n=1)
                if suggestions:
                    return f"Error: '{word}'. Did you mean '{suggestions[0]}'?"

        return None

    def check_structure(self):
        clause_order = [token for token in self.query if token in self.SQL_ORDER]

        if "SELECT" in self.query and ("FROM" not in self.query or self.query[self.query.index("SELECT") + 1] == "FROM"):
            return "Error: No columns or table specified after SELECT."
        
        if not self.validate_clause_order(clause_order):
            return f"Error: Incorrect SQL clause order."

        return None

    def validate_clause_order(self, clause_order):
        expected_index = 0
        for clause in clause_order:
            while expected_index < len(self.SQL_ORDER) and self.SQL_ORDER[expected_index] != clause:
                expected_index += 1
            if expected_index >= len(self.SQL_ORDER):
                return False
        return True

    def validate_sql(self):
        syntax_check = self.check_syntax()
        typo_check = self.check_typo()
        structure_check = self.check_structure()

        if syntax_check is not None:
            return syntax_check
        elif typo_check is not None:
            return typo_check
        elif structure_check is not None:
            return structure_check
        
        return True


# test_cases = [
#     # 1. Query Valid
#     ("SELECT name, age FROM users WHERE age > 20;", True),
#     # 2. Missing Semicolon
#     ("SELECT name, age FROM users WHERE age > 20", "Syntax Error: SQL query should end with a semicolon."),
#     # 3. Unbalanced Parentheses
#     ("SELECT name, (age FROM users WHERE age > 20;", "Error: Unbalanced parentheses in the query."),
#     # 4. Unsupported SQL Operation
#     ("FETCH name, age FROM users WHERE age > 20;", "Error: Unsupported SQL operation 'FETCH'"),
#     # # 5. Typo in SQL Keyword
#     ("SELET name, age FROM users WHERE age > 20;", "Error: 'SELET'. Did you mean 'SELECT'?"),
#     # # 6. Missing Column After SELECT
#     ("SELECT FROM users WHERE age > 20;", "Error: No columns specified after SELECT."),
#     # 7. Incorrect Clause Order
#     ("SELECT name, age WHERE age > 20 FROM users;", "Error: Incorrect SQL clause order."),
#     # 8. Valid Query with Complex Clauses
#     ("SELECT DISTINCT name, age FROM users WHERE age BETWEEN 18 AND 30 LIMIT 10;", True),
#     # 9. Typo and Unbalanced Parentheses
#     ("SELEC name, (age, FROM users WHERE age > 20;", "Error: 'SELEC'. Did you mean 'SELECT'?"),
#     # 10. Brutal SQL (Mixed Errors)
#     ("SELECT name age, (WHERE users age >20;", "Error: Unbalanced parentheses in the query."),
#     # 11. Valid Query but Lowercase
#     ("select name, age from users where age > 20;", True),
#     # 12. Query with Extra Spaces
#     ("SELECT   name  ,   age   FROM    users   WHERE   age   >  20 ;", True),
#     # 13. Completely Invalid Query
#     ("NAME AGE FROM USERS;", "Error: Unsupported SQL operation 'NAME'"),
#     # 14. Valid Query with Joins
#     ("SELECT u.name, o.order_id FROM users u JOIN orders o ON u.id = o.user_id;", True),
#     # 15. Missing FROM Clause
#     ("SELECT name, age WHERE age > 20;", "Error: Incorrect SQL clause order."),
#     # 16. Typo in Clause Name
#     ("SELECT name, age FOM users WHERE age > 20;", "Error: 'FOM'. Did you mean 'FROM'?"),
#     # 17. Query with ORDER BY but No Columns
#     ("SELECT FROM users ORDER BY ;", "Error: No columns specified after SELECT."),
#     # 18. Valid Query with Nested Parentheses
#     ("SELECT name, (age + 10) AS age_plus_ten FROM users WHERE (age > 20);", True),
#     # 19. Valid Query with LIKE Clause
#     ("SELECT name FROM users WHERE name LIKE 'A%';", True),
# ]

# # Running Test Cases
# for i, (query, expected) in enumerate(test_cases, 1):
#     checker = SQLChecker(query)
#     result = checker.validate_sql()
#     print(f"Test Case {i}: Query: {query}")
#     print(f"Expected: {expected}")
#     print(f"Result: {result}")
#     print("-" * 40)
