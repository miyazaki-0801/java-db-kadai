package kadai_007;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Posts_Capter07 {
    public static void main(String[] args) {
        // MySQLの接続URL, ユーザー名, パスワード
        String url = "jdbc:mysql://localhost:3306/challenge_java?useSSL=false&serverTimezone=UTC";
        String username = "root";
        String password = "h";

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            System.out.println("データベース接続成功：" + connection);

            // INSERT SQL
            String insertSQL = "INSERT INTO posts (user_id, posted_at, post_content, likes) VALUES (?, ?, ?, ?)";
            try (PreparedStatement insertStmt = connection.prepareStatement(insertSQL)) {

                // 追加されたレコード数をカウントする変数
                int insertCount = 0;

                // 重複チェック用SQL
                String checkSQL = "SELECT COUNT(*) FROM posts WHERE user_id = ? AND posted_at = ? AND post_content = ?";
                try (PreparedStatement checkStmt = connection.prepareStatement(checkSQL)) {

                    // 追加するデータ
                    Object[][] postData = {
                        {1003, "2023-02-08", "昨日の夜は徹夜でした・・", 13},
                        {1002, "2023-02-08", "お疲れ様です！", 12},
                        {1003, "2023-02-09", "今日も頑張ります！", 18},
                        {1001, "2023-02-09", "無理は禁物ですよ！", 17},
                        {1002, "2023-02-10", "明日から連休ですね！", 20}
                    };

                    // 各データについて処理
                    for (Object[] post : postData) {
                        int userId = (int) post[0];
                        String postedAt = (String) post[1];
                        String postContent = (String) post[2];
                        int likes = (int) post[3];

                        // 重複チェック
                        checkStmt.setInt(1, userId);
                        checkStmt.setDate(2, Date.valueOf(postedAt));
                        checkStmt.setString(3, postContent);
                        try (ResultSet rs = checkStmt.executeQuery()) {
                            rs.next();
                            int count = rs.getInt(1);
                            if (count == 0) {
                                // 既存データがない場合のみINSERT
                                insertStmt.setInt(1, userId);
                                insertStmt.setDate(2, Date.valueOf(postedAt));
                                insertStmt.setString(3, postContent);
                                insertStmt.setInt(4, likes);
                                insertCount += insertStmt.executeUpdate();
                            } else {
                                System.out.println("投稿内容が重複しているためスキップしました： " + postContent);
                            }
                        }
                    }
                }

                // 追加されたレコード数を表示
                System.out.println("レコード追加を実行します");
                System.out.println(insertCount + "件のレコードが追加されました");

            }

            // ユーザーIDが1002のレコードを検索（投稿日時が早い順に2件だけ表示）
            String selectSQL = "SELECT posted_at, post_content, likes FROM posts WHERE user_id = ? ORDER BY posted_at ASC LIMIT 2";
            try (PreparedStatement selectStmt = connection.prepareStatement(selectSQL)) {
                selectStmt.setInt(1, 1002);
                try (ResultSet rs = selectStmt.executeQuery()) {
                    System.out.println("ユーザーIDが1002のレコードを検索しました");
                    int count = 1;
                    while (rs.next()) {
                        Date postedAt = rs.getDate("posted_at");
                        String postContent = rs.getString("post_content");
                        int likeCount = rs.getInt("likes");

                        System.out.printf("%d件目：投稿日時=%s／投稿内容=%s／いいね数=%d\n",
                                count, postedAt, postContent, likeCount);
                        count++;
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("SQLエラーが発生しました: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
