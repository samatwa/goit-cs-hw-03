from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo import errors

# Підключення до MongoDB через MongoClient
client = MongoClient("mongodb+srv://samatwa:02September2009@goitlearn.l9kbfrr.mongodb.net/", server_api=ServerApi('1'))
db = client.book
cat_collection = db.cats

# Створення одного документа
def create_one_cat(cat):
    try:
        result_one = cat_collection.insert_one(cat)
        print(f"Inserted cat with id: {result_one.inserted_id}")
    except errors.PyMongoError as e:
        print(f"Error inserting cat: {e}")

# Створення кількох документів
def create_many_cats(cats):
    try:
        result_many = cat_collection.insert_many(cats)
        print(f"Inserted cats with ids: {result_many.inserted_ids}")
    except errors.PyMongoError as e:
        print(f"Error inserting cats: {e}")

# Читання всіх документів
def read_all_cats():
    try:
        cats = cat_collection.find({})
        for cat in cats:
            print(cat)
    except errors.PyMongoError as e:
        print(f"Error reading cats: {e}")

# Читання документа за ім'ям
def read_cat_by_name(name):
    try:
        cat = cat_collection.find_one({"name": name})
        if cat:
            print(cat)
        else:
            print(f"No cat found with name: {name}")
    except errors.PyMongoError as e:
        print(f"Error reading cat: {e}")

# Оновлення віку кота за ім'ям
def update_cat_age(name, new_age):
    try:
        result = cat_collection.update_one({"name": name}, {"$set": {"age": new_age}})
        if result.matched_count > 0:
            print(f"Updated age for cat named {name}")
        else:
            print(f"No cat found with name: {name}")
    except errors.PyMongoError as e:
        print(f"Error updating cat age: {e}")

# Додавання нової характеристики до кота за ім'ям
def add_feature_to_cat(name, feature):
    try:
        result = cat_collection.update_one({"name": name}, {"$push": {"features": feature}})
        if result.matched_count > 0:
            print(f"Added feature to cat named {name}")
        else:
            print(f"No cat found with name: {name}")
    except errors.PyMongoError as e:
        print(f"Error adding feature to cat: {e}")

# Видалення кота за ім'ям
def delete_cat_by_name(name):
    try:
        result = cat_collection.delete_one({"name": name})
        if result.deleted_count > 0:
            print(f"Deleted cat named {name}")
        else:
            print(f"No cat found with name: {name}")
    except errors.PyMongoError as e:
        print(f"Error deleting cat: {e}")

# Видалення всіх котів
def delete_all_cats():
    try:
        result = cat_collection.delete_many({})
        print(f"Deleted {result.deleted_count} cats")
    except errors.PyMongoError as e:
        print(f"Error deleting all cats: {e}")

# Приклад використання
if __name__ == "__main__":
    
    # Створення нового кота
    create_one_cat(
        {
            "name": "Barsik",
            "age": 3,
            "features": ["walks in slippers", "allows to be petted", "ginger"],
        }
    )
    
    # Створення кількох котів
    create_many_cats([
        {
            "name": "Lama",
            "age": 2,
            "features": ["uses litter box", "doesn't allow to be petted", "gray"],
        },
        {
            "name": "Liza",
            "age": 4,
            "features": ["uses litter box", "allows to be petted", "white"],
        }
    ])

    # Читання всіх котів
    print("\nAll cats:")
    read_all_cats()

    # Читання кота за ім'ям
    print("\nRead cat by name 'Barsik':")
    read_cat_by_name("Barsik")

    # Оновлення віку кота
    print("\nUpdate age for 'Barsik':")
    update_cat_age("Barsik", 4)

    # Додавання нової характеристики до кота
    print("\nAdd feature to 'Barsik':")
    add_feature_to_cat("Barsik", "loves to play")

    # Видалення кота за ім'ям
    print("\nDelete 'Lama':")
    delete_cat_by_name("Lama")

    # Видалення всіх котів
    print("\nDelete all cats:")
    delete_all_cats()