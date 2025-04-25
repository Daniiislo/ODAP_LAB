import csv
import pandas as pd
import os
import sys

def clear_console():
    """
    Xóa màn hình console tùy theo hệ điều hành
    """
    if sys.platform.startswith('win'):
        os.system('cls')  # Cho Windows
    else:
        os.system('clear')  # Cho macOS và Linux

#####################################################
# Câu 1: Tính tổng của các số lẻ từ 1 đến 100
#####################################################
def cau_1():
    """
    Tính tổng của các số lẻ từ 1 đến 100
    """
    sum = 0
    for i in range(1, 101, 2):  # Bước nhảy 2
        sum += i
    
    print(f"Câu 1: Tổng các số lẻ từ 1 đến 100 là: {sum}")
    return sum

#####################################################
# Câu 2: In số chia hết cho 7 nhưng không phải bội số của 5
#####################################################
def cau_2():
    """
    In ra tất cả các số chia hết cho 7 nhưng không phải là bội số của 5
    trong khoảng từ 2000 đến 3200 (bao gồm cả 2000 và 3200)
    """
    result = []
    
    for i in range(2000, 3201):
        if i % 7 == 0 and i % 5 != 0:
            result.append(i)
    
    print(f"Câu 2: Các số chia hết cho 7 nhưng không phải bội số của 5 từ 2000-3200:\n{result}")
    return result

#####################################################
# Câu 3: Đảo ngược chuỗi
#####################################################
def cau_3():
    """
    Đảo ngược một chuỗi nhập từ bàn phím
    Ví dụ: "Hello World" sẽ trở thành "dlroW olleH"
    """
    str = input("Câu 3: Nhập chuỗi cần đảo ngược: ")
    reverse_str = str[::-1]  # Sử dụng slicing để đảo ngược
    
    print(f"Chuỗi đảo ngược: {reverse_str}")
    return reverse_str

#####################################################
# Câu 4: Tính giai thừa
#####################################################
def cau_4():
    """
    Tính giai thừa của một số nguyên dương được nhập từ bàn phím
    Giai thừa của n là n! = 1 * 2 * 3 * ... * n
    """
    try:
        n = int(input("Câu 4: Nhập số nguyên dương để tính giai thừa: "))
        
        if n < 0:
            print("Vui lòng nhập số nguyên dương")
            return None
        
        factorial = 1
        for i in range(1, n + 1):
            factorial *= i
        
        print(f"Giai thừa của {n} là: {factorial}")
        return factorial
    except ValueError:
        print("Vui lòng nhập số nguyên hợp lệ")
        return None

#####################################################
# Câu 5: FizzBuzz
#####################################################
def cau_5():
    """
    Tạo danh sách các số từ 1 đến 50 và:
    - In "FizzBuzz" nếu chia hết cho cả 3 và 5
    - In "Fizz" nếu chia hết cho 3
    - In "Buzz" nếu chia hết cho 5
    """
    result = []
    
    for i in range(1, 51):
        if i % 3 == 0 and i % 5 == 0:
            result.append("FizzBuzz")
        elif i % 3 == 0:
            result.append("Fizz")
        elif i % 5 == 0:
            result.append("Buzz")
        else:
            result.append(i)
    
    print("Câu 5: Kết quả FizzBuzz từ 1-50:")
    for i, val in enumerate(result, 1):
        print(f"{i}: {val}")
    
    return result

#####################################################
# Câu 6: Kiểm tra chuỗi đối xứng
#####################################################
def cau_6():
    """
    Kiểm tra xem một chuỗi có phải là chuỗi đối xứng hay không
    Ví dụ: "radar" và "level" là chuỗi đối xứng, "hello" không phải
    """
    str = input("Câu 6: Nhập chuỗi để kiểm tra đối xứng: ")
    
    # Loại bỏ khoảng trắng và chuyển về chữ thường
    cleaned_str = str.lower().replace(" ", "")
    
    if cleaned_str == cleaned_str[::-1]:
        print(f"'{str}' là chuỗi đối xứng")
        return True
    else:
        print(f"'{str}' không phải là chuỗi đối xứng")
        return False

#####################################################
# Câu 7: Tìm số lớn nhất trong danh sách
#####################################################
def cau_7():
    """
    Xác định số lớn nhất trong một danh sách số nguyên nhập từ bàn phím
    """
    try:
        input_list = input("Câu 7: Nhập danh sách các số nguyên, cách nhau bởi dấu cách: ")
        integer_list = [int(x) for x in input_list.split()]
        
        if not integer_list:
            print("Danh sách rỗng")
            return None
        
        max_integer = max(integer_list)
        print(f"Số lớn nhất trong danh sách là: {max_integer}")
        return max_integer
    except ValueError:
        print("Vui lòng nhập các số nguyên hợp lệ")
        return None

#####################################################
# Câu 8: Xóa phần tử trùng lặp trong danh sách
#####################################################
def cau_8():
    """
    Xóa các phần tử trùng lặp trong một danh sách
    và in ra danh sách sau khi đã loại bỏ các phần tử trùng lặp
    """
    try:
        input_list = input("Câu 8: Nhập danh sách các phần tử, cách nhau bởi dấu cách: ")
        element_list = input_list.split()
        
        unique_element_list = list(dict.fromkeys(element_list))  # Sử dụng dict để giữ thứ tự
        
        print(f"Danh sách gốc: {element_list}")
        print(f"Danh sách sau khi loại bỏ trùng lặp: {unique_element_list}")
        return unique_element_list
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")
        return None

#####################################################
# Câu 9: Kiểm tra số nguyên tố
#####################################################
def cau_9():
    """
    Kiểm tra xem một số có phải là số nguyên tố hay không
    Số nguyên tố là số chỉ có hai ước số dương là 1 và chính nó
    """
#####################################################
# Câu 9: Kiểm tra số nguyên tố (sử dụng sàng nguyên tố)
#####################################################
def cau_9():
    """
    Kiểm tra xem một số có phải là số nguyên tố hay không
    Sử dụng sàng nguyên tố (Sieve of Eratosthenes)
    """
    try:
        n = int(input("Câu 9: Nhập số cần kiểm tra nguyên tố: "))
        
        if n <= 1:
            print(f"{n} không phải là số nguyên tố")
            return False
        
        # Tạo sàng nguyên tố đến n
        limit = n + 1
        prime = [True] * limit # Khởi tạo tất cả là True
        prime[0] = prime[1] = False  # 0 và 1 không phải số nguyên tố
        
        p = 2
        while p * p <= n:
            # Nếu p là số nguyên tố, đánh dấu tất cả bội số của p là không nguyên tố
            if prime[p]:
                for i in range(p * p, limit, p):
                    prime[i] = False
            p += 1
        
        # Kiểm tra n có phải số nguyên tố không
        if prime[n]:
            print(f"{n} là số nguyên tố")
            return True
        else:
            print(f"{n} không phải là số nguyên tố")
            return False
    except ValueError:
        print("Vui lòng nhập số nguyên hợp lệ")
        return None

#####################################################
# Câu 10: Tính tổng các số trong danh sách/tuple
#####################################################
def cau_10():
    """
    Tính tổng của tất cả các số trong một danh sách hoặc tuple nhập từ bàn phím
    """
    try:
        input_number_list = input("Câu 10: Nhập danh sách các số, cách nhau bởi dấu cách: ")
        number_list = [float(x) for x in input_number_list.split()]
        
        total = sum(number_list)
        print(f"Tổng các số trong danh sách là: {total}")
        return total
    except ValueError:
        print("Vui lòng nhập các số hợp lệ")
        return None

#####################################################
# Câu 11: Đọc và hiển thị dữ liệu từ file CSV
#####################################################
def cau_11():
    """
    Đọc và hiển thị dữ liệu từ file CSV:
    - Yêu cầu người dùng nhập tên file CSV
    - Đọc dữ liệu từ file CSV và lưu vào DataFrame
    - In ra dữ liệu từ file CSV
    """
    try:
        filename = input("Câu 11: Nhập tên file CSV cần đọc (bao gồm cả phần extension): ")
        
        df = pd.read_csv(filename)
        print("\nDữ liệu từ file CSV:")
        print(df)
        return df
    except Exception as e:
        print(f"Lỗi khi đọc file: {e}")
        return None

#####################################################
# Câu 12: Tính tổng/trung bình của dãy số từ file CSV
#####################################################
def cau_12():
    """
    Tính tổng hoặc trung bình của dãy số từ file CSV:
    - Yêu cầu nhập tên file CSV và tên cột chứa dãy số
    - Đọc dữ liệu từ file CSV vào DataFrame
    - Tính tổng và trung bình của dãy số
    """
    try:
        filename = input("Câu 12: Nhập tên file CSV (bao gồm cả phần extension): ")
        
        df = pd.read_csv(filename)

        print(f"Các cột có sẵn: {', '.join(df.columns)}")
        column_name = input("Nhập tên cột chứa dãy số cần tính: ")
        
        if column_name not in df.columns:
            print(f"Cột '{column_name}' không tồn tại trong file CSV.")
            return None
        
        total = df[column_name].sum()
        average = df[column_name].mean()
        
        print(f"Tổng của cột {column_name}: {total}")
        print(f"Trung bình của cột {column_name}: {average}")
        
        return {"total": total, "average": average}
    except FileNotFoundError:
        print(f"File {filename} không tồn tại.")
    except ValueError:
        print(f"Cột {column_name} không chứa dữ liệu số.")
    except Exception as e:
        print(f"Lỗi: {e}")
        return None

#####################################################
# Câu 13: Thêm dữ liệu vào file CSV
#####################################################
def cau_13():
    """
    Thêm dữ liệu vào file CSV:
    - Yêu cầu người dùng nhập tên file CSV và dữ liệu mới
    - Đọc dữ liệu hiện có từ file CSV
    - Thêm dữ liệu mới vào
    - Ghi lại dữ liệu vào file CSV
    """
    try:
        filename = input("Câu 13: Nhập tên file CSV (bao gồm cả phần extension): ")

        # Đọc dữ liệu hiện có
        df = pd.read_csv(filename)
        columns = df.columns.tolist()
        
        print(f"Các cột hiện có: {', '.join(columns)}")
        
        # Nhập dữ liệu mới
        new_data = {}
        for col in columns:
            value = input(f"Nhập giá trị cho cột '{col}': ")
            new_data[col] = value
        
        # Thêm dữ liệu mới
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)
        
        # Ghi lại vào file
        df.to_csv(filename, index=False)
        
        print(f"Đã thêm dữ liệu mới vào file {filename}")
        print(f"Dữ liệu trong file sau khi thêm:\n{df}")
        return df
    except FileNotFoundError:
        print(f"File {filename} không tồn tại.")
    except Exception as e:
        print(f"Lỗi: {e}")
        return None

# Phần chạy chương trình chính
if __name__ == "__main__":
    clear_console()
    while True:
        width = 100
        print("\n" + "=" * width)
        print("MENU BÀI TẬP PYTHON".center(width))
        print("*-*-*-*-*-*-*-*-*-*-*-*-*".center(width))
        print("Họ và tên: Nguyễn Đăng Trí".center(width))
        print("MSSV: 22120383".center(width))
        print("=" * width)
        print("1. Tính tổng các số lẻ từ 1 đến 100")
        print("2. In số chia hết cho 7 nhưng không phải bội số của 5 trong đoạn [2000;3200]")
        print("3. Đảo ngược chuỗi")
        print("4. Tính giai thừa")
        print("5. FizzBuzz")
        print("6. Kiểm tra chuỗi đối xứng")
        print("7. Tìm số lớn nhất trong danh sách")
        print("8. Xóa phần tử trùng lặp trong danh sách")
        print("9. Kiểm tra số nguyên tố")
        print("10. Tính tổng các số trong danh sách/tuple")
        print("11. Đọc và hiển thị dữ liệu từ file CSV")
        print("12. Tính tổng/trung bình của dãy số từ file CSV")
        print("13. Thêm dữ liệu vào file CSV")
        print("0. Thoát")
        print("=" * width)
        
        choice = input("Chọn bài tập (0-13): ")
        print("-" * width)
        
        if choice == '0':
            print("Cảm ơn bạn đã sử dụng chương trình! Tạm biệt.")
            break
        elif choice == '1':
            cau_1()
        elif choice == '2':
            cau_2()
        elif choice == '3':
            cau_3()
        elif choice == '4':
            cau_4()
        elif choice == '5':
            cau_5()
        elif choice == '6':
            cau_6()
        elif choice == '7':
            cau_7()
        elif choice == '8':
            cau_8()
        elif choice == '9':
            cau_9()
        elif choice == '10':
            cau_10()
        elif choice == '11':
            cau_11()
        elif choice == '12':
            cau_12()
        elif choice == '13':
            cau_13()
        else:
            print("Lựa chọn không hợp lệ. Vui lòng chọn từ 0-13.")
        
        input("\nNhấn Enter để tiếp tục...")

        clear_console()
