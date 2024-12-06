from neo4j import GraphDatabase

# Подключение к Neo4j
NEO4J_URI = "neo4j+s://d8bae317.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "vL7F5UZ4hr7sOs6qyYq8G33mZyVzmx7IPqshk5YNc_Q"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))


def run_query(query, **kwargs):
    with driver.session() as session:
        result = session.run(query, **kwargs)
        return [record.data() for record in result]


# 1. Получить последовательность остановок (названия остановки и номер по порядку) для заданного маршрута.
def stops_for_route(route):
    query = """
    MATCH (s:Stop)-[p:PART_OF]->(r:Route {number: $route})
    RETURN s.name AS stop
    """
    return run_query(query, route=route)

# 2. Получить названия организаций, расположенных рядом с заданной остановкой.
def orgs_near_stop(stop):
    query = """
    MATCH (o:Organization)-[:LOCATED_NEAR]->(s:Stop {name: $stop})
    RETURN o.name AS organization
    """
    return run_query(query, stop=stop)

# 3. Найти все названия остановок, на которых возможны пересадки на другой маршрут.
def transfer_stops():
    query = """
    MATCH (s:Stop)-[p:`PART_OF`]->(r:Route) WITH s, count(p) as count_p WHERE count_p > 1 RETURN s.name
    """
    return run_query(query)

# 4. Найти все названия остановок, на которых останавливается только один маршрут.
def single_route_stops():
    query = """
    MATCH (s:Stop)-[p:`PART_OF`]->(r:Route) WITH s, count(p) as count_p WHERE count_p = 1 RETURN s.name
    """
    return run_query(query)

# 5. Найти названия учебных организаций и названия остановок, около которых они расположены.
def educational_orgs_with_stops():
    query = """
    MATCH (o:Organization{category:"Учебное заведение"})-[p:`LOCATED_NEAR`]-(s:Stop) RETURN o.name, s.name
    """
    return run_query(query)

# 6.1 Получить все маршруты от одной заданной остановки до другой заданной остановки:
#     Остановки лежат на одном маршруте.
def routes_same_route(start, end):
    query = """
    MATCH (start:Stop {name: $start})-[:PART_OF]->(r:Route)<-[:PART_OF]-(end:Stop {name: $end})
    RETURN r.number AS routeNumber
    """
    return run_query(query, start=start, end=end)

# 7 (6.2) Получить все маршруты от одной заданной остановки до другой заданной остановки:
#     Остановки лежат на разных маршрутах.
def routes_different_routes(start, end):
    query = """
    MATCH path = (start:Stop {name: $start})-[:PART_OF*]-(end:Stop {name: $end})
    RETURN path
    """
    return run_query(query, start=start, end=end)

# 8 (7.1) Получить минимальный по количеству остановок маршрут от одной заданной остановки до другой заданной остановки:
#     Остановки лежат на одном маршруте;
def shortest_route_same(start, end):
    query = """
    MATCH path = shortestPath((start:Stop {name: $start})-[:NEXT_STOP*]-(end:Stop {name: $end})),
    (start:Stop {name: $start})-[:PART_OF]->(r),
    (end:Stop {name: $end})-[:PART_OF]->(r)
    RETURN DISTINCT path

    """
    return run_query(query, start=start, end=end)

# 9 (7.2) Получить минимальный по количеству остановок маршрут от одной заданной остановки до другой заданной остановки:
#     Остановки лежат на разных маршрутах.
def shortest_route_different(start, end):
    query = """
    MATCH path = shortestPath((start:Stop {name: $start})-[:NEXT_STOP*]-(end:Stop {name: $end}))
    RETURN path
    """
    return run_query(query, start=start, end=end)

# 10 (8). Получить все маршруты, которые проходят через 3 заданные остановки
def routes_through_three_stops(stop1, stop2, stop3):
    query = """
    MATCH (s1:Stop {name: $stop1})-[:PART_OF]->(r:Route),
      (s2:Stop {name: $stop2})-[:PART_OF]->(r),
      (s3:Stop {name: $stop3})-[:PART_OF]->(r)
    RETURN DISTINCT r.number AS routeNumber

    """
    return run_query(query, stop1=stop1, stop2=stop2, stop3=stop3)


# 11 (9). Получить маршрут, который проходит рядом с максимальным количеством магазинов.
def max_shops_stops():
    query = """
    MATCH (o:Organization {category: "Магазин"})-[:LOCATED_NEAR]->(s:Stop)-[:PART_OF]->(r:Route)
    RETURN r.number AS routeNumber, COUNT(o) AS shopCount
    ORDER BY shopCount DESC
    LIMIT 1
    """
    return run_query(query)

# 12 (10). Получить минимальный по расстоянию маршрут от одной заданной остановки до другой заданной остановки.
def min_distance_route(start, end):
    query = """
    MATCH path = shortestPath((start:Stop {name: "ул. Куйбышева"})-[r:`NEXT_STOP`*]-(end:Stop {name: "ул. Правды"}))
    WITH path, REDUCE(totalDistance = 0, rel IN relationships(path) | totalDistance + rel.distance) AS totalDistance
    RETURN path, totalDistance
    ORDER BY totalDistance ASC
    LIMIT 1
    """
    return run_query(query, start=start, end=end)

# 13(11). Найти названия организаций, расположенных рядом с третьей по счету остановкой от заданной остановки.
def third_stop_organizations(start):
    query = """
    MATCH (start:Stop {name: $start})-[:NEXT_STOP*3]->(thirdStop:Stop)
    MATCH (o:Organization)-[:LOCATED_NEAR]->(thirdStop)
    RETURN DISTINCT o.name AS organizationName, thirdStop.name
    """
    return run_query(query, start=start)

# 14 (12). Найти все маршруты, длина которых превышает 10 км.
def routes_longer_than_10():
    query = """
    MATCH (s1:Stop)-[:PART_OF]->(route:Route),
      (s1)-[rel:NEXT_STOP]->(s2:Stop)
    WITH route, SUM(rel.distance) AS totalDistance
    WHERE totalDistance > 10
    RETURN route.number AS routeNumber, totalDistance

    """
    return run_query(query)



queries = {
    "1": {"func": stops_for_route, "args": ["Введите номер маршрута: "]},
    "2": {"func": orgs_near_stop, "args": ["Введите название остановки: "]},
    "3": {"func": transfer_stops, "args": []},
    "4": {"func": single_route_stops, "args": []},
    "5": {"func": educational_orgs_with_stops, "args": []},
    "6": {"func": routes_same_route, "args": ["Введите начальную остановку: ", "Введите конечную остановку: "]},
    "7": {"func": routes_different_routes, "args": ["Введите начальную остановку: ", "Введите конечную остановку: "]},
    "8": {"func": shortest_route_same, "args": ["Введите начальную остановку: ", "Введите конечную остановку: "]},
    "9": {"func": shortest_route_different, "args": ["Введите начальную остановку: ", "Введите конечную остановку: "]},
    "10": {"func": routes_through_three_stops, "args": ["Введите первую остановку: ", "Введите вторую остановку: ", "Введите третью остановку: "]},
    "11": {"func": max_shops_stops, "args": []},
    "12": {"func": min_distance_route, "args": ["Введите начальную остановку: ", "Введите конечную остановку: "]},
    "13": {"func": third_stop_organizations, "args": ["Введите остановку: "]},
    "14": {"func": routes_longer_than_10, "args": []},
}


def display_menu():
    print("\nВыберите запрос:")
    print("1. Последовательность остановок для заданного маршрута")
    print("2. Организации рядом с заданной остановкой")
    print("3. Остановки с возможностью пересадки")
    print("4. Остановки с одним маршрутом")
    print("5. Учебные организации и остановки рядом с ними")
    print("6. Все маршруты между двумя остановками (на одном маршруте)")
    print("7. Все маршруты между двумя остановками (на разных маршрутах)")
    print("8. Маршрут с минимальным количеством остановок (на одном маршруте)")
    print("9. Маршрут с минимальным количеством остановок (на разных маршрутах)")
    print("10. Все маршруты через три заданные остановки")
    print("11. Маршрут с максимальным количеством магазинов")
    print("12. Минимальный по расстоянию маршрут")
    print("13. Организации через три остановки")
    print("14. Маршруты длинее 10км")

    print("0. Выход")


def main():
    while True:
        display_menu()
        choice = input("\nВведите номер запроса: ")

        if choice == "0":
            print("Выход из программы...")
            break

        query = queries.get(choice)
        if query:
            args = [input(prompt) for prompt in query["args"]]
            result = query["func"](*args)
            print("Результат:", result)
        else:
            print("Некорректный выбор. Попробуйте снова.")


if __name__ == "__main__":
    main()
    driver.close()
