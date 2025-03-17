import proxy_config
import osmnx as ox
import geopandas as gpd
import pandas as pd

# Ручной ввод населенного пункта
place_name = input("Введите название населённого пункта: ")

# Словарь с ключами для поиска социальных объектов (разбит по категориям)
social_tags = {
    "amenity": True,
    "building": ["public", "hospital", "school", "university", "kindergarten", "library", "museum"],
    "leisure": ["stadium", "sports_centre"],
    "healthcare": ["hospital", "clinic"],
    "office": ["government"],
    "emergency": ["fire_station"],
    "shop": ["supermarket", "convenience", "marketplace"]
}

# Запрос данных из OSM
try:
    all_poi = ox.features_from_place(place_name, social_tags)

# Проверка на ошибки в названии, ключах итд
    if all_poi.empty:
        raise ValueError("Объекты не найдены!")

    # Сброс MultiIndex, если есть
    if isinstance(all_poi.index, pd.MultiIndex):
        all_poi.reset_index(inplace=True)

    # Ищем настоящий OSM ID
    osm_id_col = None
    for col in ["osmid", "@id", "id", "osm_id"]:
        if col in all_poi.columns:
            osm_id_col = col
            break

    if osm_id_col:
        all_poi["id"] = all_poi[osm_id_col].astype(str)
    else:
        raise ValueError("Не найден столбец с OSM ID!")

    # Проверка наличия необходимых столбцов
    for col in ["name", "addr:street", "addr:housenumber", "building", "leisure", "healthcare", "office", "emergency", "shop"]:
        if col not in all_poi.columns:
            all_poi[col] = None

    # Устанавливаем CRS, если отсутствует
    if all_poi.crs is None:
        all_poi.set_crs("EPSG:4326", inplace=True)

    # Перевод в проекцию EPSG:3857 для корректного отображения координат площадных объектов
    projected_crs = "EPSG:3857"
    all_poi = all_poi.to_crs(projected_crs)

    # Добавление координаты центроидов объектов
    all_poi["geometry_type"] = all_poi.geometry.geom_type
    all_poi["x_meters"] = all_poi.geometry.centroid.x
    all_poi["y_meters"] = all_poi.geometry.centroid.y

    # Фильтр столбцов
    columns_to_export = ["id", "geometry", "geometry_type", "amenity", "name", "addr:street", "addr:housenumber", "x_meters", "y_meters"]
    optional_columns = ["building", "leisure", "healthcare", "office", "emergency", "shop"]
    existing_columns = [col for col in optional_columns if col in all_poi.columns]
    all_poi = all_poi[columns_to_export + existing_columns].reset_index(drop=True)


    output_file = f"{place_name}_social_infra.geojson"
    all_poi.to_file(output_file, driver="GeoJSON")
    print(f"Файл успешно сохранён в {output_file} ({projected_crs})")

except Exception as e:
    print(f"Ошибка при получении POI: {e}")
    exit()
# Объединение--------------------------------------------------------------------------------------

# Загрузка файлов
file1 = "poi-polygon.gpkg" #Файл из тестового задания
file2 = output_file

# Читаем данные
try:
    gdf1 = gpd.read_file(file1)
    gdf2 = gpd.read_file(file2)
except Exception as e:
    print(f"Ошибка при загрузке файлов: {e}")
    exit()

# Приведение названия колонок к нижнему регистру
gdf1.columns = [col.lower() for col in gdf1.columns]
gdf2.columns = [col.lower() for col in gdf2.columns]

# Приведение ключевую колонку id к единому названию
gdf1 = gdf1.rename(columns={"osm_id": "id"})
gdf2 = gdf2.rename(columns={"osm_id": "id"})

# Конвертация id в строку чтобы избежат ьошибок при объединении таблиц
gdf1["id"] = gdf1["id"].astype(str)
gdf2["id"] = gdf2["id"].astype(str)

# Определяем общие и уникальные колонки
common_columns = list(set(gdf1.columns) & set(gdf2.columns))
all_columns = list(set(gdf1.columns) | set(gdf2.columns))

# Объединяем данные по id (слияние строк)
merged_gdf = gdf1.merge(gdf2, on="id", how="outer", suffixes=("_gpkg", "_geojson"))

# Проверяем и корректно объединяем колонки с суффиксами
for col in common_columns:
    col_gpkg = col + "_gpkg"
    col_geojson = col + "_geojson"

    if col_gpkg in merged_gdf.columns and col_geojson in merged_gdf.columns:
        merged_gdf[col] = merged_gdf[col_gpkg].combine_first(merged_gdf[col_geojson])
        merged_gdf.drop(columns=[col_gpkg, col_geojson], inplace=True)

# Объединение геометрии, если есть дублирующиеся столбцы
if "geometry_gpkg" in merged_gdf.columns and "geometry_geojson" in merged_gdf.columns:
    merged_gdf["geometry"] = merged_gdf["geometry_gpkg"].combine_first(merged_gdf["geometry_geojson"])
    merged_gdf.drop(columns=["geometry_gpkg", "geometry_geojson"], inplace=True)

# Проверка GeoDataFrame на корректную геометрию
merged_gdf = gpd.GeoDataFrame(merged_gdf, geometry="geometry", crs=gdf1.crs if gdf1.crs else gdf2.crs)

# Преобразуем координаты в EPSG:3857 и добавляем их, если отсутствуют
if merged_gdf.crs is None or merged_gdf.crs.to_string() != "EPSG:3857":
    merged_gdf = merged_gdf.to_crs("EPSG:3857")

# Исключаем пустые строки перед вычислением центроидов
merged_gdf = merged_gdf[merged_gdf.geometry.notna()]

# Вычисляем центроиды
merged_gdf["x_meters"] = merged_gdf.geometry.centroid.x
merged_gdf["y_meters"] = merged_gdf.geometry.centroid.y

# Заменяем NaN координаты на None (Вызывал ошибку)
merged_gdf["x_meters"] = merged_gdf["x_meters"].apply(lambda x: None if pd.isna(x) else x)
merged_gdf["y_meters"] = merged_gdf["y_meters"].apply(lambda y: None if pd.isna(y) else y)

# Определяем порядок колонок
column_order = ["id", "name", "addr:street", "addr:housenumber", "x_meters", "y_meters"]
existing_columns = [col for col in column_order if col in merged_gdf.columns]
remaining_columns = [col for col in merged_gdf.columns if col not in existing_columns]
merged_gdf = merged_gdf[existing_columns + remaining_columns]

# Сохраняем в GeoJSON и CSV
output_file_geojson = "Социальные_объекты.geojson"
merged_gdf.to_file(output_file_geojson, driver="GeoJSON")
output_file_csv = "Социальные_объекты.csv"

# Убираем бесконечности и NaN
merged_gdf["x_meters"] = merged_gdf["x_meters"].replace([float("inf"), float("-inf")], None)
merged_gdf["y_meters"] = merged_gdf["y_meters"].replace([float("inf"), float("-inf")], None)
merged_gdf = merged_gdf.fillna("")

# Сохраняем с кодировкой UTF-8
merged_gdf.drop(columns=["geometry"], errors="ignore").to_csv(output_file_csv, index=False, sep=";", encoding="utf-8-sig")

print(f"Объединённые файлы сохранены как {output_file_geojson} и {output_file_csv}")