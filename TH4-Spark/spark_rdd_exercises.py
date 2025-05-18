from pyspark.sql import SparkSession
import os
import sys
import time

def clear_console():
    """
    Xóa màn hình console tùy theo hệ điều hành
    """
    if sys.platform.startswith('win'):
        os.system('cls')  # Cho Windows
    else:
        os.system('clear')  # Cho macOS và Linux

def create_spark_session():
    """
    Create a Spark session
    """
    spark = SparkSession.builder \
        .appName("Spark Exercises") \
        .getOrCreate()
    return spark

def basic_1(sc):
    print("Bài 1 (basic): Tạo một RDD từ một danh sách các số và sử dụng transformation map để tạo một RDD mới chứa bình phương của mỗi số.\n")
    numbers = [1, 2, 3, 4, 5]
    numbers_rdd = sc.parallelize(numbers)
    squared_rdd = numbers_rdd.map(lambda x: x**2)
    print("RDD gốc: ", numbers_rdd.collect())
    print("RDD bình phương: ", squared_rdd.collect())

def basic_2(sc):
    print("Bài 2 (basic): Tạo một RDD từ một danh sách các chuỗi và sử dụng transformation filter để lọc ra những chuỗi có độ dài lớn hơn 5 ký tự.\n")
    strings = ["Spark", "Hadoop", "Python", "Java", "Scala"]
    strings_rdd = sc.parallelize(strings)
    filtered_strings_rdd = strings_rdd.filter(lambda s: len(s) > 5)
    print("RDD gốc: ", strings_rdd.collect())
    print("RDD đã lọc: ", filtered_strings_rdd.collect())

def basic_3(sc):
    print("Bài 3 (basic): Tạo hai RDD riêng biệt từ hai danh sách các số và sau đó sử dụng transformation union để gộp chúng thành một RDD duy nhất.\n")
    numbers1 = [1, 2, 3]
    numbers2 = [4, 5, 6]
    numbers1_rdd = sc.parallelize(numbers1)
    numbers2_rdd = sc.parallelize(numbers2)
    union_rdd = numbers1_rdd.union(numbers2_rdd)
    print("RDD 1: ", numbers1_rdd.collect())
    print("RDD 2: ", numbers2_rdd.collect())
    print("RDD đã gộp: ", union_rdd.collect())

def basic_4(sc):
    print("Bài 4 (basic): Tạo một RDD từ một danh sách các số và sử dụng action count để đếm tổng số phần tử trong RDD.\n")
    numbers = [1, 2, 3, 4, 5]
    numbers_rdd = sc.parallelize(numbers)
    count = numbers_rdd.count()
    print("RDD gốc: ", numbers_rdd.collect())
    print("Tổng số phần tử trong RDD: ", count)

def basic_5(sc):
    print("Bài 5 (basic): Tạo một RDD từ một danh sách các chuỗi và sử dụng action first để lấy ra chuỗi đầu tiên trong RDD.\n")
    strings = ["Spark", "Hadoop", "Python", "Java", "Scala"]
    strings_rdd = sc.parallelize(strings)
    first_string = strings_rdd.first()
    print("RDD gốc: ", strings_rdd.collect())
    print("Chuỗi đầu tiên trong RDD: ", first_string)

def basic_6(sc):
    print("Bài 6 (basic): Tạo một RDD từ một danh sách các số và sử dụng action reduce để tính tổng của tất cả các số trong RDD.\n")
    numbers = [1, 2, 3, 4, 5]
    numbers_rdd = sc.parallelize(numbers)
    total_sum = numbers_rdd.reduce(lambda x, y: x + y)
    print("RDD gốc: ", numbers_rdd.collect())
    print("Tổng của tất cả các số trong RDD: ", total_sum)

def basic_7(sc):
    print("Bài 7 (basic): Tạo một RDD từ một danh sách các số và sau đó sử dụng action collect để thu thập tất cả các số trong RDD và in ra chúng.\n")
    numbers = [1, 2, 3, 4, 5]
    numbers_rdd = sc.parallelize(numbers)
    collected_numbers = numbers_rdd.collect()
    print("RDD gốc: ", numbers_rdd.collect())
    print("Các số trong RDD: ", collected_numbers)

def basic_8(sc):
    print("Bài 8 (basic): Tạo một RDD từ một danh sách các số và sử dụng transformation map để nhân mỗi số với 2, sau đó sử dụng action reduce để tính tổng của các số đã được nhân.\n")
    numbers = [1, 2, 3, 4, 5]
    numbers_rdd = sc.parallelize(numbers)
    multiplied_rdd = numbers_rdd.map(lambda x: x * 2)
    total_sum = multiplied_rdd.reduce(lambda x, y: x + y)
    print("RDD gốc: ", numbers_rdd.collect())
    print("RDD đã nhân với 2: ", multiplied_rdd.collect())
    print("Tổng của các số đã nhân với 2: ", total_sum)

def basic_9(sc):
    print("Bài 9 (basic): Tạo một RDD từ một danh sách các chuỗi và sử dụng transformation filter để lọc ra các chuỗi có chứa từ \"Spark\", sau đó sử dụng action count để đếm số lượng chuỗi thỏa điều kiện.\n")
    strings = ["Spark", "Spark SQL", "Hadoop", "Spark Streaming", "Java"]
    strings_rdd = sc.parallelize(strings)
    filtered_strings_rdd = strings_rdd.filter(lambda s: "Spark" in s)
    count = filtered_strings_rdd.count()
    print("RDD gốc: ", strings_rdd.collect())
    print("Các chuỗi có chứa từ 'Spark': ", filtered_strings_rdd.collect())
    print("Số lượng chuỗi có chứa từ 'Spark': ", count)

def basic_10(sc):
    print("Bài 10 (basic): Tạo một RDD từ một danh sách các số và sử dụng transformation map để tính bình phương của mỗi số, sau đó sử dụng action reduce để tính tổng bình phương của các số.\n")
    numbers = [1, 2, 3, 4, 5]
    numbers_rdd = sc.parallelize(numbers)
    squared_rdd = numbers_rdd.map(lambda x: x**2)
    total_sum = squared_rdd.reduce(lambda x, y: x + y)
    print("RDD gốc: ", numbers_rdd.collect())
    print("RDD bình phương: ", squared_rdd.collect())
    print("Tổng bình phương của các số: ", total_sum)

def advanced_1(sc):
    print("Bài 1 (advanced): Tạo một RDD gồm 1 triệu số nguyên. Hãy đếm số lượng partition mặc định được tạo ra. Dùng lệnh nào để thay đổi số lượng partition?\n")
    numbers = range(1, 1000001)
    numbers_rdd = sc.parallelize(numbers)
    default_partitions = numbers_rdd.getNumPartitions()
    print("Số lượng partition mặc định: ", default_partitions)
    print("Để thay đổi số lượng partition, bạn có thể sử dụng lệnh: numbers_rdd.repartition(new_num_partitions) hoặc numbers_rdd.coalesce(new_num_partitions)")
    print("Thay đổi số lượng partition thành 10 bằng cách sử dụng lệnh: numbers_rdd.repartition(10)")
    numbers_rdd = numbers_rdd.repartition(10)
    # Hoặc sử dụng coalesce để giảm số lượng partition
    #numbers_rdd = numbers_rdd.coalesce(10)
    new_partitions = numbers_rdd.getNumPartitions()
    print("Số lượng partition sau khi thay đổi: ", new_partitions)

def advanced_2(sc):
    print("Bài 2 (advanced):  So sánh hiệu suất xử lý khi thực hiện count() trước và sau khi dùng .cache(). Hãy nêu sự khác biệt?\n")
    numbers = range(1, 1000001)
    numbers_rdd = sc.parallelize(numbers)
    # Tính toán mà không sử dụng cache
    start_time = time.time()
    count_before_cache = numbers_rdd.count()
    end_time = time.time()
    print("Số lượng phần tử trong RDD (trước khi cache): ", count_before_cache)
    print("Thời gian xử lý (trước khi cache): ", end_time - start_time)

    # Sử dụng cache
    numbers_rdd.cache()
    # Tính toán lại sau khi sử dụng cache
    start_time = time.time()
    count_after_cache = numbers_rdd.count()
    end_time = time.time()
    print("\nSố lượng phần tử trong RDD (sau khi cache): ", count_after_cache)
    print("Thời gian xử lý (sau khi cache): ", end_time - start_time)

    print("Sự khác biệt: Khi sử dụng cache, Spark sẽ lưu trữ RDD trong bộ nhớ để tránh tính toán lại khi thực hiện các action nhiều lần. Điều này giúp tăng tốc độ xử lý cho các action sau đó.")
          
def advanced_3(sc):
    print("Bài 3 (advanced):  Sử dụng glom() để in kích thước của từng partition. Hãy nhận xét kết quả đó?\n")

    numbers = range(1, 1000001)
    numbers_rdd = sc.parallelize(numbers, 10)  # Tạo RDD với 10 partition
    print("Tạo RĐD gồm 1 triệu số nguyên với 10 partition.")
    glommed_rdd = numbers_rdd.glom()
    partition_sizes = glommed_rdd.map(len).collect()
    print("Kích thước của từng partition: ", partition_sizes)
    print("Nhận xét: Kích thước của từng partition có thể khác nhau tùy thuộc vào cách mà dữ liệu được phân phối. Nếu dữ liệu không được phân phối đồng đều, một số partition có thể chứa nhiều phần tử hơn những partition khác.")

def advanced_4(sc):
    print("Bài 4 (advanced):  Tạo một RDD gồm các cặp (key, value), trong đó key là một chuỗi, value là số nguyên. Hãy tính tổng giá trị theo từng key.\n")
    pairs = [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)]
    pairs_rdd = sc.parallelize(pairs)
    # Sử dụng reduceByKey để tính tổng giá trị theo từng key
    total_by_key_rdd = pairs_rdd.reduceByKey(lambda x, y: x + y)
    print("RDD gốc: ", pairs_rdd.collect())
    print("Tổng giá trị theo từng key: ", total_by_key_rdd.collect())

def advanced_5(sc):
    print("Bài 5 (advanced): Với dữ liệu key-value, hãy tính trung bình cộng giá trị cho từng key.\n")
    pairs = [("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)]
    pairs_rdd = sc.parallelize(pairs)
    # # Sử dụng combineByKey để tính trung bình cộng giá trị cho từng key
    def create_combiner(value):
        return (value, 1)
    def merge_value(accumulator, value):
        return (accumulator[0] + value, accumulator[1] + 1)
    def merge_combiners(accumulator1, accumulator2):
        return (accumulator1[0] + accumulator2[0], accumulator1[1] + accumulator2[1])
    def calculate_average(accumulator):
        return accumulator[0] / accumulator[1] if accumulator[1] != 0 else 0
    
    combined_rdd = pairs_rdd.combineByKey(create_combiner, merge_value, merge_combiners)
    average_rdd = combined_rdd.mapValues(calculate_average)
    print("RDD gốc: ", pairs_rdd.collect())
    print("Trung bình cộng giá trị cho từng key: ", average_rdd.collect())

def advanced_6(sc):
    print("Bài 6 (advanced): Tạo một RDD gồm các từ trong nhiều câu văn. Hãy đếm số lần xuất hiện của mỗi từ.\n")
    sentences = ["Spark is great", "Hadoop is also great", "Python is great for data science"]
    print("Danh sách các câu: ", sentences)
    # Tạo RDD từ danh sách các câu
    words = " ".join(sentences).split()
    words_rdd = sc.parallelize(words)
    word_count_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
    print("RDD gốc: ", words_rdd.collect())
    print("Số lần xuất hiện của mỗi từ: ", word_count_rdd.collect())

def advanced_7(sc):
    print("Bài 7 (advanced): Sử dụng flatMap để biến một RDD chứa các chuỗi thành RDD chứa từng từ riêng lẻ.\n")
    sentences = ["Spark is great", "Hadoop is also great", "Python is great for data science"]
    sentences_rdd = sc.parallelize(sentences)
    words_rdd = sentences_rdd.flatMap(lambda sentence: sentence.split())
    print("RDD gốc: ", sentences_rdd.collect())
    print("RDD chứa từng từ riêng lẻ: ", words_rdd.collect())

def advanced_8(sc):
    print("Bài 8 (advanced): Dùng distinct() để loại bỏ các phần tử trùng nhau trong một RDD. Sau đó kết hợp với map để đếm độ dài mỗi phần tử duy nhất.\n")
    words = ["Spark", "Hadoop", "Python", "Java", "Scala", "Spark"]
    words_rdd = sc.parallelize(words)
    distinct_rdd = words_rdd.distinct()
    length_rdd = distinct_rdd.map(lambda word: (word, len(word)))
    print("RDD gốc: ", words_rdd.collect())
    print("RDD sau khi loại bỏ phần tử trùng nhau: ", distinct_rdd.collect())
    print("Độ dài mỗi phần tử duy nhất: ", length_rdd.collect())

def advanced_9(sc):
    print("Bài 9 (advanced): Tạo một RDD gồm các số từ 1 đến 1000. Lọc ra các số chia hết cho cả 3 và 5, rồi tính tổng các số đó.\n")
    numbers = range(1, 1001)
    numbers_rdd = sc.parallelize(numbers)
    filtered_rdd = numbers_rdd.filter(lambda x: x % 3 == 0 and x % 5 == 0)
    total_sum = filtered_rdd.reduce(lambda x, y: x + y)
    print("RDD gốc: ", numbers_rdd.collect())
    print("\nCác số chia hết cho cả 3 và 5: ", filtered_rdd.collect())
    print("\nTổng các số chia hết cho cả 3 và 5: ", total_sum)

def advanced_10(sc):
    print("Bài 10 (advanced):  Cho một RDD chứa danh sách tên sinh viên. Hãy lọc ra những sinh viên có họ bắt đầu bằng chữ “Nguyễn” và đếm số lượng.\n")
    students = ["Nguyễn Văn A", "Trần Thị B", "Nguyễn Thị C", "Lê Văn D"]
    students_rdd = sc.parallelize(students)
    filtered_students_rdd = students_rdd.filter(lambda name: name.startswith("Nguyễn"))
    count = filtered_students_rdd.count()
    print("RDD gốc: ", students_rdd.collect())
    print("Danh sách sinh viên có họ bắt đầu bằng chữ 'Nguyễn': ", filtered_students_rdd.collect())
    print("Số lượng sinh viên có họ bắt đầu bằng chữ 'Nguyễn': ", count)

def advanced_11(sc):
    print("Bài 11 (advanced):  Hãy giải thích sự khác biệt giữa reduceByKey và groupByKey. Trong trường hợp nào nên dùng reduceByKey?\n")
    print("reduceByKey: Làm giảm các giá trị theo từng key bằng cách sử dụng một hàm giảm. Nó thực hiện việc giảm ngay lập tức trên các partition và sau đó gộp kết quả lại.")
    print("groupByKey: Gộp tất cả các giá trị theo từng key lại với nhau mà không thực hiện giảm ngay lập tức. Điều này có thể dẫn đến việc truyền tải nhiều dữ liệu hơn giữa các partition.")
    print("Nên sử dụng reduceByKey khi muốn giảm thiểu dữ liệu truyền tải giữa các partition và chỉ cần một giá trị duy nhất cho mỗi key. groupByKey thường tốn kém hơn về hiệu suất.")

def advanced_12(sc):
    print("Bài 12 (advanced): Giả sử cần tính tổng giá trị các phần tử theo nhóm, nhóm được xác định bởi điều kiện phân loại (ví dụ: chẵn/lẻ). Hãy nêu giải pháp tiếp cận?\n")
    print("Giải pháp: Sử dụng map để gán nhãn cho từng phần tử theo nhóm (chẵn/lẻ), sau đó sử dụng reduceByKey để tính tổng giá trị cho từng nhóm. Ví dụ:\n")
    print("numbers_rdd.map(lambda x: ('even', x) if x % 2 == 0 else ('odd', x)).reduceByKey(lambda x, y: x + y)\n")
    print("=> Tạo ra một RDD với các cặp (nhóm, tổng) cho từng nhóm.")

def advanced_13(sc):
    print("Bài 13 (advanced): 13. Với một đoạn văn bản dài (có thể copy từ một bài báo), hãy viết một pipeline Spark để:" \
    "\nChuyển văn bản thành danh sách các từ." \
    "\nLọc bỏ stop words (tự định nghĩa danh sách)." \
    "\nĐếm số lần xuất hiện của mỗi từ còn lại." \
    "\nSắp xếp giảm dần theo số lần xuất hiện và in ra top 10 từ phổ biến nhất.\n")

    text = "Spark is a unified analytics engine for big data processing, with built-in modules for streaming, SQL, machine learning and graph processing." \
    "It is designed to be fast and general-purpose. Spark can be used for a wide range of applications, including batch processing, interactive queries, real-time analytics, and machine learning." \
    "It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. Spark is widely used in the industry for big data processing and analytics."

    print("Đoạn văn bản gốc: ", text)

    stop_words = ["is", "a", "for", "the", "and", "to", "in", "of", "that", "it", "can", "be", "used", "as"]
    print("\nDanh sách stop words: ", stop_words)

    # Chuyển văn bản thành danh sách các từ
    words = text.split()
    words_rdd = sc.parallelize(words)
    print("\nDanh sách các từ: ", words_rdd.collect())

    # Lọc bỏ stop words
    filtered_words_rdd = words_rdd.filter(lambda word: word.lower() not in stop_words)
    print("\nDanh sách các từ sau khi lọc bỏ stop words: ", filtered_words_rdd.collect())

    # Đếm số lần xuất hiện của mỗi từ còn lại
    word_count_rdd = filtered_words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
    print("\nSố lần xuất hiện của mỗi từ còn lại sau khi lọc bỏ stop words: ", word_count_rdd.collect())

    # Sắp xếp giảm dần theo số lần xuất hiện và in ra top 10 từ phổ biến nhất
    sorted_word_count_rdd = word_count_rdd.sortBy(lambda x: x[1], ascending=False)
    top_10_words = sorted_word_count_rdd.take(10)
    print("\nTop 10 từ phổ biến nhất: ", top_10_words)

def advanced_14(sc):
    print("Bài 14 (advanced): Hãy viết một pipeline Spark để tính:" \
    "\nTổng giá trị các số chia hết cho 7 từ 1 đến 10 triệu." \
    "\nSo sánh thời gian thực hiện khi chia dữ liệu thành 2, 4, và 8 partition.")

    numbers = range(1, 10000001)
    partition_counts = [2, 4, 8]

    for partition_count in partition_counts:
        print(f"\nSố lượng partition: {partition_count}")
        numbers_rdd = sc.parallelize(numbers, partition_count)

        # Tính tổng giá trị các số chia hết cho 7
        start_time = time.time()
        filtered_rdd = numbers_rdd.filter(lambda x: x % 7 == 0)
        total_sum = filtered_rdd.reduce(lambda x, y: x + y)
        end_time = time.time()

        print("Tổng giá trị các số chia hết cho 7: ", total_sum)
        print("Thời gian thực hiện: ", end_time - start_time)


def basic_menu(sc):
    width = 30
    while True:
        clear_console()
        print("=" * width)
        print("MENU BÀI TẬP CƠ BẢN".center(width))
        print("=" * width)
        print("1.\tBài 1")
        print("2.\tBài 2")
        print("3.\tBài 3")
        print("4.\tBài 4")
        print("5.\tBài 5")
        print("6.\tBài 6")
        print("7.\tBài 7")
        print("8.\tBài 8")
        print("9.\tBài 9")
        print("10.\tBài 10")
        print("11.\tQuay lại menu chính")
        print("=" * width)

        choice = input("Chọn bài tập (1-11): ")
        print("=" * width)
        if choice == "1":
            basic_1(sc)
        elif choice == "2":
            basic_2(sc)
        elif choice == "3":
            basic_3(sc)
        elif choice == "4":
            basic_4(sc)
        elif choice == "5":
            basic_5(sc)
        elif choice == "6":
            basic_6(sc)
        elif choice == "7":
            basic_7(sc)
        elif choice == "8":
            basic_8(sc)
        elif choice == "9":
            basic_9(sc)
        elif choice == "10":
            basic_10(sc)
        elif choice == "11":
            break
        else:
            print("Lựa chọn không hợp lệ. Vui lòng chọn lại.")

        input("\nNhấn Enter để tiếp tục...")

def advanced_menu(sc):
    width = 30
    while True:
        clear_console()
        print("=" * width)
        print("MENU BÀI TẬP NÂNG CAO".center(width))
        print("=" * width)
        print("1.\tBài 1")
        print("2.\tBài 2")
        print("3.\tBài 3")
        print("4.\tBài 4")
        print("5.\tBài 5")
        print("6.\tBài 6")
        print("7.\tBài 7")
        print("8.\tBài 8")
        print("9.\tBài 9")
        print("10.\tBài 10")
        print("11.\tBài 11")
        print("12.\tBài 12")
        print("13.\tBài 13")
        print("14.\tBài 14")
        print("15.\tQuay lại menu chính")
        print("=" * width)

        choice = input("Chọn bài tập (1-15): ")
        print("=" * width)
        if choice == "1":
            advanced_1(sc)
        elif choice == "2":
            advanced_2(sc)
        elif choice == "3":
            advanced_3(sc)
        elif choice == "4":
            advanced_4(sc)
        elif choice == "5":
            advanced_5(sc)
        elif choice == "6":
            advanced_6(sc)
        elif choice == "7":
            advanced_7(sc)
        elif choice == "8":
            advanced_8(sc)
        elif choice == "9":
            advanced_9(sc)
        elif choice == "10":
            advanced_10(sc)
        elif choice == "11":
            advanced_11(sc)
        elif choice == "12":
            advanced_12(sc)
        elif choice == "13":
            advanced_13(sc)
        elif choice == "14":
            advanced_14(sc)
        elif choice == "15":
            break
        else:
            print("Lựa chọn không hợp lệ. Vui lòng chọn lại.")

        input("\nNhấn Enter để tiếp tục...")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    while True:
        clear_console()
        width = 30
        print("=" * width)
        print("MENU BÀI TẬP SPARK".center(width))
        print("*-*-*-*-*-*-*-*-*-*-*-*-*".center(width))
        print("Họ và tên: Nguyễn Đăng Trí".center(width))
        print("MSSV: 22120383".center(width))
        print("=" * width)
        print("1. Bài tập cơ bản")
        print("2. Bài tập nâng cao")
        print("3. Thoát")
        print("=" * width)
        choice = input("Chọn bài tập (1-3): ")
        if choice == "1":
            clear_console()
            basic_menu(sc)
        elif choice == "2":
            clear_console()
            advanced_menu(sc)
        elif choice == "3":
            print("Cảm ơn bạn đã sử dụng chương trình!")
            break
        else:
            print("Lựa chọn không hợp lệ. Vui lòng chọn lại.")

    spark.stop()

if __name__ == "__main__":
    main()
