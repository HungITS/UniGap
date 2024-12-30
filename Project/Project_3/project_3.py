import pandas as pd
from datetime import datetime

def convert_datetime(date):
    # Chuyển đổi chuỗi sang datetime
    date_obj = pd.to_datetime(date, format='%m/%d/%y', errors='coerce')
    current_year = datetime.now().year  # Lấy năm hiện tại
    # Kiểm tra và chỉnh năm
    if date_obj.year > current_year:  # Nếu năm lớn hơn năm hiện tại, giảm 100 năm
        date_obj = date_obj.replace(year=date_obj.year - 100)
    return date_obj


if __name__ == '__main__':
    # Đọc dữ liệu
    df = pd.read_csv('tmdb-movies.csv')

    # Sắp xếp các bộ phim theo ngày phát hành giảm dần rồi lưu ra một file mới
    df['release_date'] = df['release_date'].apply(convert_datetime)
    df1 = df.sort_values(by = 'release_date', ascending=False)
    df1.to_csv('movies_sort_by_realease_date.csv')
    
    # Lọc ra các bộ phim có đánh giá trung bình trên 7.5 rồi lưu ra một file mới
    df['vote_average'] = df['vote_average'].astype('float')
    df2 = df.query('vote_average >= 7.5')
    df2.to_csv('movies_sort_by_vote_average.csv')

    # Tìm ra phim nào có doanh thu cao nhất và doanh thu thấp nhất
    max_revenue = df['revenue'].max()  
    highest_revenue_movie = df[df['revenue'] == max_revenue] 
    print("Phim có doanh thu cao nhất:")
    print(highest_revenue_movie)


    min_revenue = df['revenue'].min()  
    lowest_revenue_movie = df[df['revenue'] == min_revenue]
    print("Phim có doanh thu thấp nhất:")
    print(lowest_revenue_movie)

    # Tính tổng doanh thu tất cả các bộ phim
    sum_revenue = sum(df['revenue'])
    print("Tổng doanh thu tất cả các bộ phim: ", sum_revenue)

    # Lấy Top 10 bộ phim có doanh thu cao nhất
    top_10_movies = df.nlargest(10, 'revenue')
    print("Top 10 bộ phim có doanh thu cao nhất:")
    print(top_10_movies)

    # Đạo diễn nào có nhiều bộ phim nhất và diễn viên nào đóng nhiều phim nhất
    director_counts = df['director'].value_counts()
    top_director = director_counts.sort_values(ascending=False).head(1)
    print('Đạo diễn có nhiều bộ phim nhất là:')
    print(top_director)

    actor_counts = df['cast'].str.split('|').explode().str.strip().value_counts()
    top_actor = actor_counts.sort_values(ascending=False).head(1)
    print('Diễn viên có nhiều bộ phim nhất là:')
    print(top_actor)

    # Thống kê số lượng phim theo các thể loại.
    genre_counts = df['genres'].str.split('|').explode().str.strip().value_counts()
    print(genre_counts)