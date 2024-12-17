#!bin/bash

DATA="/media/hungits/Study/Self_Study/UniGap/Project/Project_1/tmdb-movies.csv"

# 1.Sắp xếp các bộ phim theo ngày phát hành hành giảm dần
sort -t, -k12,12r "$DATA" > movies_sort_by_realease_date.csv

# 2.Lọc các bộ phim có điểm >7.5
awk -F, '$15 > 7.5' "$DATA" > movies_rating_gt_75.csv

# 3.Tìm phim có doanh thu cao nhất và thấp nhất
echo "Phim doanh thu cao nhất"
sort -t. -k5,5nr "$DATA" | head -n 1
echo "Phim doanh thu thấp nhất"
sort -t, -k5,5n "$DATA" | head -n 1

# 4.Tìm tổng doanh thu
echo "Chưa giải quyết được tìm tổng doanh thu"

# 5. Top 10 bộ phim doanh thu nhiều nhất
echo "Top 10 bộ phim có doanh thu nhiều nhất"
sort -t, -k5,5nr "$DATA" | head -n 10 > top_10_revenue.csv

# 6.Đạo diễn nào có nhiều bộ phim nhất và diễn viên nào đóng nhiều phim nhất
echo "Đạo diễn có nhiều phim nhất:"
cut -d, -f9 "$DATA" |sort| uniq -c | sort -nr | head -n 1 
echo "Diễn viên đóng nhiều phim nhất:"
cut -d, -f7 "$DATA" | tr '|' '\n' | sort | uniq -c | sort -nr | head -n 1

# 7. Thống kê số lượng phim theo thể loạicsv
echo "Thống kê phim theo thể loại:"
cut -d, -f11 "$DATA" | tr '|' '\n' | sort | uniq -c | sort -nr > genres.csv

