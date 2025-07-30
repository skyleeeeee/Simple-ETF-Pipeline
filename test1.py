from demo.cleaning import get_users, save_users, transform_and_validate 






if __name__ == "__main__":
    data = get_users()
    save_users(data)
    output_path, count = transform_and_validate()
    print(f"Created file at {output_path} with {count} records")