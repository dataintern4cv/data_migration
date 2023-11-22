
class QueryGenerator:

    ORACLE_CONTRAINTS = [
        ("countries", "fk_countries_regions"),
        ("locations", "fk_locations_countries"),
        ("warehouses", "fk_warehouses_locations"),
        ("employees", "fk_employees_manager"),
        ("products", "fk_products_categories"),
        ("contacts", "fk_contacts_customers"),
        ("orders", "fk_orders_customers"),
        ("orders", "fk_orders_employees"),
        ("order_items", "fk_order_items_products"),
        ("order_items", "fk_order_items_orders"),
        ("inventories", "fk_inventories_products"),
        ("inventories", "fk_inventories_warehouses"),
    ]

    POSTGRE_CONTRAINTS = [

        ("contacts", "fk_contacts_customers", "customer_id", "customers", "customer_id"),
        ("countries", "fk_countries_regions", "region_id", "regions", "region_id"),
        ("inventories", "fk_inventories_products", "product_id", "products", "product_id"),
        ("inventories", "fk_inventories_warehouses", "warehouse_id", "warehouses", "warehouse_id"),
        ("locations", "fk_locations_countries", "country_id", "countries", "country_id"),
        ("order_items", "fk_order_items_orders", "order_id", "orders", "order_id"),
        ("order_items", "fk_order_items_products", "product_id", "products", "product_id"),
        ("orders", "fk_orders_customers", "customer_id", "customers", "customer_id"),
        ("products", "fk_products_categories", "category_id", "product_categories", "category_id"),
        ("warehouses", "fk_warehouses_locations", "location_id", "locations", "location_id")
    ]


    @staticmethod
    def generate_insert_statement(input_data, table_name):

        query_list = []
        for index, row in input_data.iterrows():
            columns = ', '.join(row.index)

            values_list = []

            for val in row.values:
                formatted_val = f"'{val}'" if isinstance(val, str) else str(val)
                values_list.append(formatted_val)

            values = ', '.join(values_list)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            query_list.append(query)

        return query_list

    @staticmethod
    def generate_delete_statements(table_names: list[str]):

        query_list = []


        # check type and generate strings for querry
        for table in table_names:
            query = f"TRUNCATE TABLE {table}"
            query_list.append(query)

        return query_list

    @staticmethod
    def generate_update_statement(table_name, set_column, set_value, condition_column, condition_value):

        # check type and generate strings for querry
        if isinstance(set_value, (int, float)):
            set_value_str = str(set_value)
        else:
            set_value_str = f"'{set_value}'"

        if isinstance(condition_value, (int, float)):
            condition_value_str = str(condition_value)
        else:
            condition_value_str = f"'{condition_value}'"

        query = f"UPDATE {table_name} SET {set_column} = {set_value_str} WHERE {condition_column} = {condition_value_str}"

        return query

    @staticmethod
    def generate_disable_constraints(service: str):

        query_list = []
        if service == 'ORACLE':
            for table_name, constraint_name in QueryGenerator.ORACLE_CONTRAINTS:
                statement = f"ALTER TABLE {table_name} DISABLE CONSTRAINT {constraint_name}"
                query_list.append(statement)
        elif service == 'POSTGRE':
            for table_name, constraint_name, _, _, _ in QueryGenerator.POSTGRE_CONTRAINTS:
                statement = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {constraint_name}"
                query_list.append(statement)

        return query_list

    @staticmethod
    def generate_enable_constraints(service: str):

        query_list = []
        if service == 'ORACLE':

            for table_name, constraint_name in QueryGenerator.ORACLE_CONTRAINTS:
                statement = f"ALTER TABLE {table_name} ENABLE CONSTRAINT {constraint_name}"
                query_list.append(statement)

        elif service == 'POSTGRE':

            for table_name, constraint_name, column_name, ref_table, ref_column in QueryGenerator.POSTGRE_CONTRAINTS:
                statement = f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column_name}) REFERENCES {ref_table} ({ref_column})"
                query_list.append(statement)

        return query_list

