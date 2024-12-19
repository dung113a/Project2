# Đường dẫn tới file
file_path = "Project2.txt"
search_id = "50071890"  # ID cần tìm

try:
    # Mở file và đọc từng dòng
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Tìm vị trí của ID
    for index, line in enumerate(lines, start=1):  # Dòng bắt đầu từ 1
        if line.strip() == search_id:
            print(f"ID {search_id} được tìm thấy tại dòng {index}")
            break
    else:
        print(f"ID {search_id} không tồn tại trong file {file_path}")

except FileNotFoundError:
    print(f"Không tìm thấy file: {file_path}")
except Exception as e:
    print(f"Có lỗi xảy ra: {e}")
