# Reflection — Day 18 Lakehouse Lab

## Anti-pattern dễ mắc nhất: Small-file accumulation

Anti-pattern mà team tôi dễ vướng nhất là **tích lũy quá nhiều file nhỏ**
(small-file problem), đặc biệt trong bối cảnh pipeline streaming hoặc
micro-batch viết liên tục vào Bronze layer.

Nguyên nhân thực tế: khi deadline gấp, team thường ưu tiên "data vào được
là ổn" — mỗi batch nhỏ tạo ra một Parquet file riêng, không ai để ý đến số
lượng file tích lũy. Sau vài tuần, Bronze table có hàng nghìn file nhỏ vài
KB, query scan toàn bộ thay vì dùng min/max stats để prune — hiệu năng
sụp đổ nhưng không ai biết tại sao.

Lab NB2 chứng minh điều này rõ ràng: trước OPTIMIZE là hàng trăm file nhỏ,
sau OPTIMIZE + Z-ORDER thì query speedup ≥ 3×. Con số này đủ gây production
incident nếu bảng lớn cỡ TB.

Bài học: OPTIMIZE không phải "nice to have" — cần lên lịch compaction định
kỳ (ví dụ: mỗi giờ cho hot tables, mỗi ngày cho cold tables) ngay từ khi
thiết kế pipeline, không phải chờ khi đã chậm mới xử lý.
