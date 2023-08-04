import csv
import logging
import os
import re
import numpy as np
import sqlparse
from sqlparse.tokens import Token
import itertools

from ensemble_compilation.graph_representation import Query, QueryType, AggregationType, AggregationOperationType

logger = logging.getLogger(__name__)


def _extract_identifiers(tokens, enforce_single=True):
    identifiers = [token for token in tokens if isinstance(token, sqlparse.sql.IdentifierList)]
    if len(identifiers) >= 1:
        if enforce_single:
            assert len(identifiers) == 1
        identifiers = identifiers[0]
    else:
        identifiers = [token for token in tokens if isinstance(token, sqlparse.sql.Identifier)]
    return identifiers


# Find corresponding table of attribute
def _find_matching_table(attribute, schema, alias_dict):
    table_name = None
    for table_obj in schema.tables:
        if table_obj.table_name not in alias_dict.values():
            continue
        if attribute in table_obj.attributes:
            table_name = table_obj.table_name

    assert table_name is not None, f"No table found for attribute {attribute}."
    return table_name


def _fully_qualified_attribute_name(identifier, schema, alias_dict, return_split=False):
    if len(identifier.tokens) == 1:
        attribute = identifier.tokens[0].value
        table_name = _find_matching_table(attribute, schema, alias_dict)
        if not return_split:
            return table_name + '.' + attribute
        else:
            return table_name, attribute

    # Replace alias by full table names
    assert identifier.tokens[1].value == '.', "Invalid Identifier"
    if not return_split:
        return alias_dict[identifier.tokens[0].value] + '.' + identifier.tokens[2].value
    else:
        return alias_dict[identifier.tokens[0].value], identifier.tokens[2].value


def _parse_aggregation(alias_dict, function, query, schema):
    operation_factors = []
    operation_type = None
    operator = _extract_identifiers(function.tokens)[0]
    if operator.normalized == 'sum' or operator.normalized == 'SUM':
        operation_type = AggregationType.SUM
    elif operator.normalized == 'avg' or operator.normalized == 'AVG':
        operation_type = AggregationType.AVG
    elif operator.normalized == 'count' or operator.normalized == 'COUNT':
        query.add_aggregation_operation((AggregationOperationType.AGGREGATION, AggregationType.COUNT, []))
        return
    else:
        raise Exception(f"Unknown operator: {operator.normalized} ")
    operand_parantheses = [token for token in function if isinstance(token, sqlparse.sql.Parenthesis)]
    assert len(operand_parantheses) == 1
    operand_parantheses = operand_parantheses[0]
    operation_tokens = [token for token in operand_parantheses
                        if isinstance(token, sqlparse.sql.Operation)]
    # Product of columns
    if len(operation_tokens) == 1:
        operation_tokens = operation_tokens[0].tokens
        assert [token.value == ' ' or token.value == '*' for token in operation_tokens
                if not isinstance(token, sqlparse.sql.Identifier)], \
            "Currently multiplication is the only supported operator."
        identifiers = _extract_identifiers(operation_tokens)
        for identifier in identifiers:
            feature = _fully_qualified_attribute_name(identifier, schema, alias_dict, return_split=True)
            operation_factors.append(feature)
    # single column
    else:
        feature = _fully_qualified_attribute_name(_extract_identifiers(operand_parantheses)[0], schema,
                                                  alias_dict, return_split=True)
        operation_factors.append(feature)
    query.add_aggregation_operation((AggregationOperationType.AGGREGATION, operation_type, operation_factors))


def parse_what_if_query(query_str, schema, return_condition_string=False):
    assert query_str.startswith("WHAT IF"), "Not a valid what if query"
    query_str = query_str.replace("WHAT IF", "")

    # find out factor
    if "DECREASE BY" in query_str:
        percentage_change = -1
        condition_string, percentage = query_str.split("DECREASE BY")
    elif "INCREASE BY" in query_str:
        percentage_change = 1
        condition_string, percentage = query_str.split("INCREASE BY")
    else:
        raise ValueError("Not a valid what if query")
    percentage = float(percentage.strip(" %;")) / 100
    percentage_change *= percentage

    # parse condtions
    parsed_conditions = []
    conditions = condition_string.split(" AND ")
    for condition in conditions:
        if "=" in condition:
            operator = "="
            column, where_condition = condition.split("=", 1)
        elif "IN" in condition:
            operator = "IN"
            column, where_condition = condition.split("IN", 1)
        else:
            raise NotImplementedError

        column = column.strip()
        where_condition = where_condition.strip()

        if "." in column:
            table, attribute = column.split(".", 1)
        else:
            table = _find_matching_table(column, schema,
                                         {table.table_name: table.table_name for table in schema.tables})
            attribute = column

        parsed_conditions.append((table, attribute + " " + operator + " " + where_condition))

    if return_condition_string:
        return parsed_conditions, percentage_change, condition_string
    return parsed_conditions, percentage_change


def all_operations_of_type(type, query):
    return all([aggregation_type == type for aggregation_operation_type, aggregation_type, _ in
                query.aggregation_operations if
                aggregation_operation_type == AggregationOperationType.AGGREGATION])


def _match_join_condition(query, tables, schema):
    for key1, values1 in tables.items():
        for key2, values2 in tables.items():
            if key1 == key2:
                continue
            else:
                for value1 in values1:
                    for value2 in values2:
                        if value1.split('_')[1] == value2.split('_')[1]:
                            left_part = key1 + '.' + value1
                            right_part = key2 + '.' + value2
                            if left_part + ' = ' + right_part in schema.relationship_dictionary.keys():
                                query.add_join_condition(left_part + ' = ' + right_part)
                            elif right_part + ' = ' + left_part in schema.relationship_dictionary.keys():
                                query.add_join_condition(right_part + ' = ' + left_part)


def _regex_match_schema(tables, schema_path='Benchmark/tpch/schema.py'):
    schema_dict = {}
    with open(schema_path, 'r+') as f:
        lines = f.readlines()
        for line in lines:
            pattern = r'(\w+)\s*=\s*\{record\({(.*?)}\)'
            match = re.search(pattern, line)
            if match:
                variable_name = match.group(1).replace("_type", "")
                fields = re.findall(r'"(.*?)"', match.group(2))
                if variable_name in tables:
                    schema_dict[variable_name] = fields

    return schema_dict


def _regex_match_tables(query):
    pattern = r'@sdql_compile\((.*?)\)'
    match = re.search(pattern, query)
    if match:
        dict_str = match.group(1)
        dict_str = re.sub(r'\'|\"', '', dict_str)
        kv_pairs = re.findall(r'(\w+):\s?(\w+)', dict_str)
        matched_tables = {value.replace("_type", ""): key for i, (key, value) in enumerate(kv_pairs)}
        decay_factor = len(kv_pairs) - len(matched_tables)
        if len(matched_tables) >= 4 and 'supplier' in matched_tables \
            and list(matched_tables) != ['part', 'supplier', 'partsupp', 'nation', 'region']:
            matched_tables.pop('supplier')
            decay_factor = 0.5

    return matched_tables, decay_factor


def _regex_match_where_condition(query):
    normalized_query = ' '.join([line.strip() for line in query.splitlines()])
    matched_where_conditions = {}

    pattern = r"p\[\d+\]\.[\w_]+\s*==\s*p\[\d+\]\.[\w_]+"
    normalized_query = re.sub(pattern, "", normalized_query)

    pattern = r'(\w+)\s*==\s*(\w+)'
    matches = re.findall(pattern, normalized_query)
    for match in matches:
        if match[1].islower():
            mapping_pattern = rf'{match[1]}\s*=\s*\"([^"]+)\"'
            mapping_match = re.findall(mapping_pattern, normalized_query)
            if mapping_match:
                if match[0] in matched_where_conditions:
                    matched_where_conditions[match[0]].append(mapping_match[0])
                else:
                    matched_where_conditions[match[0]] = [mapping_match[0]]
        elif match[1].isdigit():
            if match[0] in matched_where_conditions:
                matched_where_conditions[match[0]].append(match[1])
            else:
                matched_where_conditions[match[0]] = [match[1]]

    return matched_where_conditions


def _regex_match_condition(query):
    normalized_query = ' '.join([line.strip() for line in query.splitlines()])
    matched_where_conditions = {}

    pattern = r"p\[\d+\]\.[\w_]+\s*<\s*p\[\d+\]\.[\w_]+"
    normalized_query = re.sub(pattern, "", normalized_query)
    pattern = r"p\[\d+\]\.[\w_]+\s*<=\s*p\[\d+\]\.[\w_]+"
    normalized_query = re.sub(pattern, "", normalized_query)
    pattern = r"p\[\d+\]\.[\w_]+\s*>\s*p\[\d+\]\.[\w_]+"
    normalized_query = re.sub(pattern, "", normalized_query)
    pattern = r"p\[\d+\]\.[\w_]+\s*>=\s*p\[\d+\]\.[\w_]+"
    normalized_query = re.sub(pattern, "", normalized_query)

    pattern = r'(\w+)\s*>=\s*(\d+\.*\d*)'
    matches = re.findall(pattern, normalized_query)
    for match in matches:
        if match:
            if match[0] in matched_where_conditions:
                matched_where_conditions[match[0]].append(f"{match[0]}>={match[1]}")
            else:
                matched_where_conditions[match[0]] = [f"{match[0]}>={match[1]}"]

    pattern = r'(\w+)\s*>\s*(\d+\.*\d*)'
    matches = re.findall(pattern, normalized_query)
    for match in matches:
        if match:
            if match[0] in matched_where_conditions:
                matched_where_conditions[match[0]].append(f"{match[0]}>{match[1]}")
            else:
                matched_where_conditions[match[0]] = [f"{match[0]}>{match[1]}"]

    pattern = r'(\w+)\s*<=\s*(\d+\.*\d*)'
    matches = re.findall(pattern, normalized_query)
    for match in matches:
        if match:
            if match[0] in matched_where_conditions:
                matched_where_conditions[match[0]].append(f"{match[0]}<={match[1]}")
            else:
                matched_where_conditions[match[0]] = [f"{match[0]}<={match[1]}"]

    pattern = r'(\w+)\s*<\s*(\d+\.*\d*)'
    matches = re.findall(pattern, normalized_query)
    for match in matches:
        if match:
            if match[0] in matched_where_conditions:
                matched_where_conditions[match[0]].append(f"{match[0]}<{match[1]}")
            else:
                matched_where_conditions[match[0]] = [f"{match[0]}<{match[1]}"]

    for key, values in matched_where_conditions.items():
        gt, st = [], []
        gte_cnt, ste_cnt, gt_cnt, st_cnt = 0, 0, 0, 0
        condition_list = []
        for value in values:
            if '>=' in value:
                gt.append(float(value.split('>=')[1]))
                gte_cnt += 1
            elif '>' in value:
                gt.append(float(value.split('>')[1]))
                gt_cnt += 1
            elif '<=' in value:
                st.append(float(value.split('<=')[1]))
                ste_cnt += 1
            elif '<' in value:
                st.append(float(value.split('<')[1]))
                st_cnt += 1
            else:
                condition_list.append(value)

        if gt:
            if gte_cnt >= gt_cnt:
                condition_list.append(f'{key}>={np.percentile(gt, 1)}')
            else:
                condition_list.append(f'{key}>{np.percentile(gt, 1)}')

        if st:
            if ste_cnt >= st_cnt:
                condition_list.append(f'{key}<={np.percentile(st, 99)}')
            else:
                condition_list.append(f'{key}<{np.percentile(st, 99)}')

        matched_where_conditions[key] = condition_list

    return matched_where_conditions


def _regex_match_output(query, table_abbreviation_dict):
    normalized_query = ' '.join([line.strip() for line in query.splitlines()])
    pattern = r'results\s*=\s*\w+\.\w+\(\s*(.*?)\s*\)'
    match = re.search(pattern, normalized_query)
    output_fields = []

    if match:
        output_fields = re.findall(r'"([^"]*)"', match.group(1))
    if len(output_fields) == 0:
        pattern = r'results\s*=\s*\w+\(\s*(.*?)\s*\)'
        match = re.search(pattern, normalized_query)
        if match:
            output_fields = re.findall(r'"([^"]*)"', match.group(1))
    if len(output_fields) == 0:
        matched_things = [filter_thing for filter_thing in re.findall(r'"([^"]+)"', normalized_query) if filter_thing not in list(table_abbreviation_dict.values())]
        output_fields = matched_things[:2] + matched_things[-2:]

    return output_fields


def parse_sdql_query(query_str, schema):
    """
    Parses SDQL queries and returns cardinality query object.
    :param query_str:
    :param schema:
    :return:
    """
    query = Query(schema)
    table_abbreviation_dict, decay_factor = _regex_match_tables(query_str)
    schema_dict = _regex_match_schema(tables=table_abbreviation_dict)
    output_fields = _regex_match_output(query_str, table_abbreviation_dict)
    all_variables = list(itertools.chain(*list(schema_dict.values())))
    groupby_variables = [variable for variable in output_fields if variable in all_variables]
    other_variables = [variable for variable in output_fields if variable not in all_variables]
    tables = [(table, table_abbreviation) for table, table_abbreviation in table_abbreviation_dict.items() if table in schema_dict]
    identifiers = list(itertools.chain(*tables))
    identifier_token_length = len(identifiers) // 2
    _match_join_condition(query, schema_dict, schema)
    alias_dict = dict()
    for table, alias in tables:
        query.table_set.add(table)
        alias_dict[alias] = table
    matched_conditions_dict = _regex_match_condition(query_str)
    matched_where_conditions_dict = _regex_match_where_condition(query_str)

    table_where_condition_dict = dict()
    for table, columns in schema_dict.items():
        for column in columns:
            if column in matched_where_conditions_dict:
                if table in table_where_condition_dict:
                    if len(matched_where_conditions_dict[column]) == 1:
                        table_where_condition_dict[table].append(f'{column}={matched_where_conditions_dict[column][0]}')
                    else:
                        table_where_condition_dict[table].append(f'{column} IN {set(matched_where_conditions_dict[column])}'.replace("{", "(").replace("}", ")"))
                else:
                    if len(matched_where_conditions_dict[column]) == 1:
                        table_where_condition_dict[table] = [f'{column}={matched_where_conditions_dict[column][0]}']
                    else:
                        table_where_condition_dict[table] = [f'{column} IN {set(matched_where_conditions_dict[column])}'.replace("{", "(").replace("}", ")")]

    for table, columns in schema_dict.items():
        for column in columns:
            if column in matched_conditions_dict:
                for condition in matched_conditions_dict[column]:
                    if table in table_where_condition_dict:
                        table_where_condition_dict[table].append(f'{condition}')
                    else:
                        table_where_condition_dict[table] = [f'{condition}']

    for key, values in table_where_condition_dict.items():
        for value in values:
            query.add_where_condition(key, value)

    query.query_type = QueryType.CARDINALITY
    for variable in groupby_variables:
        table_name = _find_matching_table(variable, schema, alias_dict)
        query.add_group_by(table_name, variable)

    return query, decay_factor


def parse_query(query_str, schema):
    """
    Parses simple SQL queries and returns cardinality query object.
    :param query_str:
    :param schema:
    :return:
    """
    query = Query(schema)

    # split query into part before from
    parsed_tokens = sqlparse.parse(query_str)[0]
    from_idxs = [i for i, token in enumerate(parsed_tokens) if token.normalized == 'FROM']
    assert len(from_idxs) == 1, "Nested queries are currently not supported."
    from_idx = from_idxs[0]
    tokens_before_from = parsed_tokens[:from_idx]

    # split query into part after from and before group by
    group_by_idxs = [i for i, token in enumerate(parsed_tokens) if token.normalized == 'GROUP BY']
    assert len(group_by_idxs) == 0 or len(group_by_idxs) == 1, "Nested queries are currently not supported."
    group_by_attributes = None
    if len(group_by_idxs) == 1:
        tokens_from_from = parsed_tokens[from_idx:group_by_idxs[0]]
        order_by_idxs = [i for i, token in enumerate(parsed_tokens) if token.normalized == 'ORDER BY']
        if len(order_by_idxs) > 0:
            group_by_end = order_by_idxs[0]
            tokens_group_by = parsed_tokens[group_by_idxs[0]:group_by_end]
        else:
            tokens_group_by = parsed_tokens[group_by_idxs[0]:]
        # Do not enforce single because there could be order by statement. Will be ignored.
        group_by_attributes = _extract_identifiers(tokens_group_by, enforce_single=False)
    else:
        tokens_from_from = parsed_tokens[from_idx:]

    # Get identifier to obtain relevant tables
    identifiers = _extract_identifiers(tokens_from_from)
    identifier_token_length = \
        [len(token.tokens) for token in identifiers if isinstance(token, sqlparse.sql.Identifier)][0]

    if identifier_token_length == 3:
        # (title, t)
        tables = [(token[0].value, token[2].value) for token in identifiers if
                  isinstance(token, sqlparse.sql.Identifier)]
    else:
        # (title, title), no alias
        tables = [(token[0].value, token[0].value) for token in identifiers if
                  isinstance(token, sqlparse.sql.Identifier)]
    alias_dict = dict()
    decay_factor = 0
    for i, (table, alias) in enumerate(tables):
        if len(tables) >= 4 and table == 'supplier' \
        and list(tables) != [('part', 'part'), ('supplier', 'supplier'), ('partsupp', 'partsupp'), ('nation', 'nation'), ('region', 'region')]:
            decay_factor = 0.5
            continue
        query.table_set.add(table)
        alias_dict[alias] = table

    # If there is a group by clause, parse it
    if group_by_attributes is not None:

        identifier_token_length = \
            [len(token.tokens) for token in _extract_identifiers(group_by_attributes)][0]

        if identifier_token_length == 3:
            # lo.d_year
            group_by_attributes = [(alias_dict[token[0].value], token[2].value) for token in
                                   _extract_identifiers(group_by_attributes)]
            for table, attribute in group_by_attributes:
                query.add_group_by(table, attribute)
        else:
            # d_year
            for group_by_token in _extract_identifiers(group_by_attributes):
                attribute = group_by_token.value
                table = _find_matching_table(attribute, schema, alias_dict)
                query.add_group_by(table, attribute)

    # Obtain projection/ aggregation attributes
    count_statements = [token for token in tokens_before_from if
                        token.normalized == 'COUNT(*)' or token.normalized == 'count(*)']
    assert len(count_statements) <= 1, "Several count statements are currently not supported."
    if len(count_statements) == 1:
        query.query_type = QueryType.CARDINALITY
    else:
        query.query_type = QueryType.AQP
        identifiers = _extract_identifiers(tokens_before_from)

        # Only aggregation attribute, e.g. sum(lo_extendedprice*lo_discount)
        if not isinstance(identifiers, sqlparse.sql.IdentifierList):
            handle_aggregation(alias_dict, query, schema, tokens_before_from)
        # group by attributes and aggregation attribute
        else:
            handle_aggregation(alias_dict, query, schema, identifiers.tokens)

    # Obtain where statements
    where_statements = [token for token in tokens_from_from if isinstance(token, sqlparse.sql.Where)]
    assert len(where_statements) <= 1
    if len(where_statements) == 0:
        return query

    where_statements = where_statements[0]
    assert len(
        [token for token in where_statements if token.normalized == 'OR']) == 0, "OR statements currently unsupported."

    # Parse where statements
    # parse multiple values differently because sqlparse does not parse as comparison
    in_statements = [idx for idx, token in enumerate(where_statements) if token.normalized == 'IN']
    for in_idx in in_statements:
        assert where_statements.tokens[in_idx - 1].value == ' '
        assert where_statements.tokens[in_idx + 1].value == ' '
        # ('bananas', 'apples')
        possible_values = where_statements.tokens[in_idx + 2]
        assert isinstance(possible_values, sqlparse.sql.Parenthesis)
        # fruits
        identifier = where_statements.tokens[in_idx - 2]
        assert isinstance(identifier, sqlparse.sql.Identifier)

        if len(identifier.tokens) == 1:

            left_table_name, left_attribute = _fully_qualified_attribute_name(identifier, schema, alias_dict,
                                                                              return_split=True)
            query.add_where_condition(left_table_name, left_attribute + ' IN ' + possible_values.value)
        else:
            assert identifier.tokens[1].value == '.', "Invalid identifier."
            # Replace alias by full table names
            query.add_where_condition(alias_dict[identifier.tokens[0].value],
                                      identifier.tokens[2].value + ' IN ' + possible_values.value)
    # normal comparisons
    comparisons = [token for token in where_statements if isinstance(token, sqlparse.sql.Comparison)]
    for comparison in comparisons:
        left = comparison.left
        assert isinstance(left, sqlparse.sql.Identifier), "Invalid where condition"
        comparison_tokens = [token for token in comparison.tokens if token.ttype == Token.Operator.Comparison]
        assert len(comparison_tokens) == 1, "Invalid comparison"
        operator_idx = comparison.tokens.index(comparison_tokens[0])

        if len(left.tokens) == 1:
            if str(left).split('_')[0] == 's' and list(tables) != [('part', 'part'), ('supplier', 'supplier'), ('partsupp', 'partsupp'), ('nation', 'nation'), ('region', 'region')]:
                continue
            left_table_name, left_attribute = _fully_qualified_attribute_name(left, schema, alias_dict,
                                                                              return_split=True)
            left_part = left_table_name + '.' + left_attribute
            right = comparison.right
            if str(right).split('_')[0] == 's' and list(tables) != [('part', 'part'), ('supplier', 'supplier'), ('partsupp', 'partsupp'), ('nation', 'nation'), ('region', 'region')]:
                continue
            # Join relationship
            if isinstance(right, sqlparse.sql.Identifier):
                assert len(right.tokens) == 1, "Invalid Identifier"

                right_attribute = right.tokens[0].value
                right_table_name = _find_matching_table(right_attribute, schema, alias_dict)
                right_part = right_table_name + '.' + right_attribute

                if left_part + ' = ' + right_part in schema.relationship_dictionary.keys():
                    query.add_join_condition(left_part + ' = ' + right_part)
                elif right_part + ' = ' + left_part in schema.relationship_dictionary.keys():
                    query.add_join_condition(right_part + ' = ' + left_part)

            # Where condition
            else:
                where_condition = left_attribute + "".join(
                    [token.value.strip() for token in comparison.tokens[operator_idx:]])
                if 'IN(' in where_condition:
                    column, condition = where_condition.split('IN')
                    query.add_where_condition(left_table_name, column + ' IN ' + condition)
                else:
                    query.add_where_condition(left_table_name, where_condition)
        else:
            # Replace alias by full table names
            left_part = _fully_qualified_attribute_name(left, schema, alias_dict)

            right = comparison.right
            # Join relationship
            if isinstance(right, sqlparse.sql.Identifier):
                assert right.tokens[1].value == '.', "Invalid Identifier"
                right_part = alias_dict[right.tokens[0].value] + '.' + right.tokens[2].value
                assert comparison.tokens[operator_idx].value == '=', "Invalid join condition"
                # assert left_part + ' = ' + right_part in schema.relationship_dictionary.keys() or \
                #        right_part + ' = ' + left_part in schema.relationship_dictionary.keys(), "Relationship unknown"
                if left_part + ' = ' + right_part in schema.relationship_dictionary.keys():
                    query.add_join_condition(left_part + ' = ' + right_part)
                elif right_part + ' = ' + left_part in schema.relationship_dictionary.keys():
                    query.add_join_condition(right_part + ' = ' + left_part)

            # Where condition
            else:
                query.add_where_condition(alias_dict[left.tokens[0].value],
                                          left.tokens[2].value + comparison.tokens[operator_idx].value + right.value)

    for i, (table1, alias1) in enumerate(tables):
        if len(tables) >= 4 and table1 == 'supplier':
            continue
        for j, (table2, alias2) in enumerate(tables):
            if len(tables) >= 4 and table2 == 'supplier':
                continue
            if table1 == table2:
                continue
            for relationship in schema.relationship_dictionary.keys():
                if table1 in relationship and table2 in relationship:
                    query.add_join_condition(relationship)

    return query, decay_factor


def handle_aggregation(alias_dict, query, schema, tokens_before_from):
    operations = [token for token in tokens_before_from if isinstance(token, sqlparse.sql.Operation)]
    assert len(operations) <= 1, "A maximum of 1 operation is supported."
    if len(operations) == 0:
        functions = [token for token in tokens_before_from if isinstance(token, sqlparse.sql.Function)]
        assert len(functions) == 1, "Only a single aggregate function is supported."
        function = functions[0]
        _parse_aggregation(alias_dict, function, query, schema)
    else:
        operation = operations[0]
        inner_operations = [token for token in operation.tokens if isinstance(token, sqlparse.sql.Operation)]
        # handle inner operations recursively
        if len(inner_operations) > 0:
            assert len(inner_operations) == 1, "Multiple inner operations impossible"
            handle_aggregation(alias_dict, query, schema, inner_operations)
        for token in operation.tokens:
            if isinstance(token, sqlparse.sql.Function):
                _parse_aggregation(alias_dict, token, query, schema)
            elif token.value == '-':
                query.add_aggregation_operation((AggregationOperationType.MINUS, None, None))
            elif token.value == '+':
                query.add_aggregation_operation((AggregationOperationType.PLUS, None, None))


def save_csv(csv_rows, target_csv_path):
    os.makedirs(os.path.dirname(target_csv_path), exist_ok=True)
    logger.info(f"Saving results to {target_csv_path}")

    with open(target_csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, csv_rows[0].keys())
        for i, row in enumerate(csv_rows):
            if i == 0:
                w.writeheader()
            w.writerow(row)
