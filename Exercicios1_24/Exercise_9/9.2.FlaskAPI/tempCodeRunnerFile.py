   # Write the data into a JSON file
        file_name = f"sales_{product_id}_{user_id}_{sales_ts.replace(':', '').replace(' ', '_')}.json"
        file_path = os.path.join("json_files", file_name)

        # Ensure the folder 'json_files' exists
        os.makedirs("json_files", exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)