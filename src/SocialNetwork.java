import java.sql.*;
import java.util.Date;

public class SocialNetwork {

    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCAL_NAME = "localhost:5432/";

    private static final String DATABASE_NAME = "socialNetwork";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCAL_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "admin";
    public static final java.sql.Date NOW = new java.sql.Date(new Date().getTime());

    public static void main(String[] args) {

        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {

            System.out.println("Переписка двух пользователей:");
            getMessagesBetweenUsers(connection, 1, 5);
            System.out.println("Все комментарии под постом:");
            getCommentsByPosts(connection, 5);
            System.out.println("Все посты конкретного пользователя:");
            getPostsByUser(connection, 1);
            System.out.println("Автор конкретного поста:");
            getPostAuthor(connection, 3);
            System.out.println("Количество лайков под конкретным постом:");
            getLikesCount(connection, 5);
            System.out.println("Количество друзей по убыванию:");
            getUsersWithMostFriends(connection);
            System.out.println("Количество лайков на посте по убыванию:");
            getPostsWithMostLikes(connection);

            //addUser(connection, "Дарья", "Зеленина", java.sql.Date.valueOf("1984-11-30"), "jshearer@att.net", "1KsEu6Oo22tf");
            //addUser(connection, "Петр", "Иванов", java.sql.Date.valueOf("2000-01-12"), "petrIvanov@yandex.ru", "123123");
            //addComments(connection, "мне нравится", 1, 3);
            //addPost(connection, "Что?", 6);
            //addLike(connection, 5, 7);
            //addFriend(connection, 9, 6);
            //addComments(connection, "где?", 5, 9);
            //sendMessage(connection, "Привет, да нормуль", 5, 1);
            //updatePost(connection, 1, 1, "Всем привет!");

            //deleteAllPosts(connection, 1);
//            addFriend(connection, 1, 3);
            //addPost(connection, "Всем привет хочу пригласить вас в свой блог!", NOW, 1);
//            deleteAllPosts(connection, 1);
//
            //addComments(connection, "Я хочу!", 1, 3);
//
//            addLike(connection, 1, 1);
//
//            addFriend(connection, 1, 2);
//
//            sendMessage(connection, "Привет. Как дела?", NOW, 1, 3);
//            sendMessage(connection, "Привет. Всё хорошо", NOW, 3, 1);
//
//            getMessagesBetweenUsers(connection, 1, 3);

//            getPostsWithMostLikes(connection);
//            getUsersWithMostFriends(connection);
//            getCommentsByPosts(connection, 1);
//
//            getUserActions(connection, 1);

            //deleteUserCascade(connection, 1);
            //deleteUserCascade(connection, 2);

            System.out.println("Список всех пользователей: ");
            getUsers(connection);

            System.out.println("Список всех постов: ");
            getPosts(connection);

            System.out.println("Список всех сообщений: ");
            getMessages(connection);

            System.out.println("Список всех лайков: ");
            getLikes(connection);

            System.out.println("Список всех друзей: ");
            getFriends(connection);

            System.out.println("Список всех комментариев: ");
            getComments(connection);

        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23514"))
                System.out.println("Неверный формат email");
            else if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных"); //ошибки на добавление в бд
            } else throw new RuntimeException(e);
        }
    }

    //Проверка подключения БД
    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }
    //Reset
    private static void resetUserIdSequenceIfTableEmpty(Connection connection) throws SQLException {
        String checkTableEmptySQL = "SELECT COUNT(*) FROM users";
        String resetSequenceSQL = "ALTER SEQUENCE users_id_seq RESTART WITH 1";

        try (PreparedStatement checkStatement = connection.prepareStatement(checkTableEmptySQL);
             ResultSet resultSet = checkStatement.executeQuery()) {

            if (resultSet.next()) {
                int userCount = resultSet.getInt(1);
                if (userCount == 0) {
                    try (PreparedStatement resetStatement = connection.prepareStatement(resetSequenceSQL)) {
                        resetStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при сбросе последовательности ID: " + e.getMessage());
            throw e;
        }
    }

    private static void resetCommentIdSequenceIfTableEmpty(Connection connection) throws SQLException {
        String checkTableEmptySQL = "SELECT COUNT(*) FROM comments";
        String resetSequenceSQL = "ALTER SEQUENCE comments_id_seq RESTART WITH 1";

        try (PreparedStatement checkStatement = connection.prepareStatement(checkTableEmptySQL);
             ResultSet resultSet = checkStatement.executeQuery()) {

            if (resultSet.next()) {
                int commentCount = resultSet.getInt(1);
                if (commentCount == 0) {
                    try (PreparedStatement resetStatement = connection.prepareStatement(resetSequenceSQL)) {
                        resetStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при сбросе последовательности ID комментариев: " + e.getMessage());
            throw e;
        }
    }

    private static void resetPostIdSequenceIfTableEmpty(Connection connection) throws SQLException {
        String checkTableEmptySQL = "SELECT COUNT(*) FROM posts";
        String resetSequenceSQL = "ALTER SEQUENCE posts_id_seq RESTART WITH 1";

        try (PreparedStatement checkStatement = connection.prepareStatement(checkTableEmptySQL);
             ResultSet resultSet = checkStatement.executeQuery()) {

            if (resultSet.next()) {
                int postCount = resultSet.getInt(1);
                if (postCount == 0) {
                    try (PreparedStatement resetStatement = connection.prepareStatement(resetSequenceSQL)) {
                        resetStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при сбросе последовательности ID постов: " + e.getMessage());
            throw e;
        }
    }

    private static void resetLikeIdSequenceIfTableEmpty(Connection connection) throws SQLException {
        String checkTableEmptySQL = "SELECT COUNT(*) FROM likes";
        String resetSequenceSQL = "ALTER SEQUENCE likes_id_seq RESTART WITH 1";

        try (PreparedStatement checkStatement = connection.prepareStatement(checkTableEmptySQL);
             ResultSet resultSet = checkStatement.executeQuery()) {

            if (resultSet.next()) {
                int likeCount = resultSet.getInt(1);
                if (likeCount == 0) {
                    try (PreparedStatement resetStatement = connection.prepareStatement(resetSequenceSQL)) {
                        resetStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при сбросе последовательности ID лайков: " + e.getMessage());
            throw e;
        }
    }

    private static void resetMessageIdSequenceIfTableEmpty(Connection connection) throws SQLException {
        String checkTableEmptySQL = "SELECT COUNT(*) FROM messages";
        String resetSequenceSQL = "ALTER SEQUENCE messages_id_seq RESTART WITH 1";

        try (PreparedStatement checkStatement = connection.prepareStatement(checkTableEmptySQL);
             ResultSet resultSet = checkStatement.executeQuery()) {

            if (resultSet.next()) {
                int messageCount = resultSet.getInt(1);
                if (messageCount == 0) {
                    try (PreparedStatement resetStatement = connection.prepareStatement(resetSequenceSQL)) {
                        resetStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при сбросе последовательности ID сообщений: " + e.getMessage());
            throw e;
        }
    }

    private static void resetFriendIdSequenceIfTableEmpty(Connection connection) throws SQLException {
        String checkTableEmptySQL = "SELECT COUNT(*) FROM friends";
        String resetSequenceSQL = "ALTER SEQUENCE friends_id_seq RESTART WITH 1";

        try (PreparedStatement checkStatement = connection.prepareStatement(checkTableEmptySQL);
             ResultSet resultSet = checkStatement.executeQuery()) {

            if (resultSet.next()) {
                int friendCount = resultSet.getInt(1);
                if (friendCount == 0) {
                    try (PreparedStatement resetStatement = connection.prepareStatement(resetSequenceSQL)) {
                        resetStatement.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при сбросе последовательности ID друзей: " + e.getMessage());
            throw e;
        }
    }

    //Гетеры
    private static void getUsers(Connection connection) throws SQLException{
        String columnName0 = "id", columnName1 = "first_name", columnName2 = "last_name", columnName3 = "birth_date", columnName4 = "email", columnName5 = "password";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM users ORDER BY id;");

        while (rs.next()) {
            String param5 = rs.getString(columnName5);
            String param4 = rs.getString(columnName4);
            java.sql.Date param3 = rs.getDate(columnName3);
            String param2 = rs.getString(columnName2);
            String param1 = rs.getString(columnName1);
            int param0 = rs.getInt(columnName0);
            System.out.println("ID: " + param0 + " | Имя: " + param1 + " | Фамилия: " + param2
                    + " | Дата рождения: " + param3 + " | email: " + param4 + " | password: " + param5);
        }
        System.out.println();
    }

    private static void getPosts(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "text", columnName2 = "create_date", columnName3 = "author_id";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM posts ORDER BY id");

        while (rs.next()) {
            int param3 = rs.getInt(columnName3);
            java.sql.Date param2 = rs.getDate(columnName2);
            String param1 = rs.getString(columnName1);
            int param0 = rs.getInt(columnName0);
            System.out.println("ID: " + param0 + " | Text: " + param1 + " | Дата написания: " + param2 + " | ID автора: " + param3);
        }
        System.out.println();
    }

    private static void getMessages(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "text", columnName2 = "sent_date", columnName3 = "sender_id", columnName4 = "receiver_id";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM messages ORDER BY id");

        while (rs.next()) {
            int param4 = rs.getInt(columnName4);
            int param3 = rs.getInt(columnName3);
            java.sql.Date param2 = rs.getDate(columnName2);
            String param1 = rs.getString(columnName1);
            int param0 = rs.getInt(columnName0);
            System.out.println("ID: " + param0 + " | Text: " + param1 + " | Дата написания: " + param2 + " | ID отправлявшего: " + param3 + " | ID получателя: " + param4);
        }
        System.out.println();
    }

    private static void getLikes(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "post_id", columnName2 = "author_id";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM likes ORDER BY id");

        while(rs.next()) {
            int param2 = rs.getInt(columnName2);
            int param1 = rs.getInt(columnName1);
            int param0 = rs.getInt(columnName0);
            System.out.println("ID: " + param0 + " |" + " ID поста: " + param1 + " |" + " ID автора лайка: " + param2);
        }
        System.out.println();
    }

    private static void getFriends(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "user_id", columnName2 = "friend_id";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM friends ORDER BY id");

        while(rs.next()) {
            int param2 = rs.getInt(columnName2);
            int param1 = rs.getInt(columnName1);
            int param0 = rs.getInt(columnName0);
            System.out.println("ID: " + param0 + " | ID пользователя добавившего в друзья: " + param1 + " | ID пользователя добавленного в друзья: " + param2);
        }
        System.out.println();
    }

    private static void getComments(Connection connection) throws SQLException {
        String columnName0 = "id", columnName1 = "text", columnName2 = "creation_date", columnName3 = "post_id", columnName4 = "author_id";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM comments ORDER BY id");

        while(rs.next()) {
            int param4 = rs.getInt(columnName4);
            int param3 = rs.getInt(columnName3);
            java.sql.Date param2 = rs.getDate(columnName2);
            String param1 = rs.getString(columnName1);
            int param0 = rs.getInt(columnName0);
            System.out.println("ID: " + param0 + " | Text: " + param1 + " | Дата написания комментария: " + param2 + " | ID поста: " + param3 + " | ID автора: " + param4);
        }
        System.out.println();
    }

    private static void getLikesCount(Connection connection, int post_id) throws SQLException {
        if (postVerification(connection, post_id)) {
            System.out.println("Пост c ID: " + post_id + " не найден");
            return;
        }
        String query = "SELECT COUNT(*) FROM likes WHERE post_id = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, post_id);
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                System.out.println("Количество лайков: " + rs.getInt(1) + " | на посте c ID: " + post_id);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получении количество лайков на посте: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void getPostAuthor(Connection connection, int post_id) throws SQLException {
        if (postVerification(connection, post_id)) {
            System.out.println("Пост c ID: " + post_id + " не найден");
            return;
        }
        String query = "SELECT u.id, u.first_name, u.last_name FROM users u " +
                "JOIN posts p ON u.id = p.author_id WHERE p.id = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, post_id);
            ResultSet rs = preparedStatement.executeQuery();
            if (rs.next()) {
                System.out.println("ID: " + rs.getInt("id") + " | Имя: " + rs.getString("first_name")
                        + " | Фамилия: " + rs.getString("last_name"));
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получения автора поста: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void getPostsByUser(Connection connection, int user_id) throws SQLException {
        if (userVerification(connection, user_id)) {
            System.out.println("Пользователя с ID: " + user_id + " не найден");
            return;
        }
        String query = "SELECT id, text, create_date FROM posts WHERE author_id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)){
            preparedStatement.setInt(1, user_id);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                int post_id = rs.getInt("id");
                String text = rs.getString("text");
                Date create_date = rs.getDate("create_date");
                System.out.println("ID: " + post_id + " | текст: " + text + " | Дата написания: " + create_date);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получении постов по ID: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void getCommentsByPosts(Connection connection, int post_id) throws SQLException {
        if (postVerification(connection, post_id)) {
            System.out.println("Пост с ID: " + post_id + " не найден");
            return;
        }
        String query = "SELECT id, text, creation_date, author_id FROM comments WHERE post_id = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, post_id);
            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                int comments_id = rs.getInt("id");
                String text = rs.getString("text");
                Date create_date = rs.getDate("creation_date");
                int author_id = rs.getInt("author_id");
                System.out.println("ID: " + comments_id + " | текст: " + text + " | дата написания: " + create_date + " | ID автора: " + author_id);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получении комментариев под постом: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void getMessagesBetweenUsers(Connection connection, int user_id1, int user_id2) throws SQLException {
        resetMessageIdSequenceIfTableEmpty(connection);
        if (userVerification(connection, user_id1) || userVerification(connection, user_id2)) {
            System.out.println("Пользователь c этим ID не найден");
            return;
        }
        String query = "SELECT id, text, sent_date, sender_id, receiver_id FROM messages WHERE (sender_id = ? AND receiver_id = ?)" +
                " OR (sender_id = ? AND receiver_id = ?)";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, user_id1);
            preparedStatement.setInt(2, user_id2);
            preparedStatement.setInt(3, user_id2);
            preparedStatement.setInt(4, user_id1);

            ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                int message_id = rs.getInt("id");
                String text = rs.getString("text");
                Date sentDate = rs.getDate("sent_date");
                int sender_id = rs.getInt("sender_id");
                int receiver_id = rs.getInt("receiver_id");
                System.out.println("ID сообщения: " + message_id + " | текст: " + text + " | дата написания сообщения: "
                        + sentDate + " | ID отправителя: " + sender_id + " | ID получателя: " + receiver_id);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получении сообщений между пользователями: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void getPostsWithMostLikes(Connection connection) throws SQLException {
        String query = "SELECT p.id, p.text, COUNT(l.id) as like_count " +
                "FROM posts p " +
                "LEFT JOIN likes l ON p.id = l.post_id " +
                "GROUP BY p.id " +
                "ORDER BY like_count DESC";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()) {
                int post_id = rs.getInt("id");
                String text = rs.getString("text");
                int likeCount = rs.getInt("like_count");
                System.out.println("ID Поста: " + post_id + " | текст: " + text + " | количество лайков: " + likeCount);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получении поста с наибольшим количеством лайков: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void getUsersWithMostFriends(Connection connection) throws SQLException {
        String query = "SELECT u.id, u.first_name, u.last_name, COUNT(f.friend_id) as friend_count " +
                "FROM users u " +
                "LEFT JOIN friends f ON u.id = f.user_id " +
                "GROUP BY u.id " +
                "ORDER BY friend_count DESC";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()) {
                int user_id = rs.getInt("id");
                String firstName = rs.getString("first_name");
                String lastName = rs.getString("last_name");
                int friendCount = rs.getInt("friend_count");
                System.out.println("ID Пользователя: " + user_id + " | Имя: " + firstName + " | Фамилия: " + lastName + " | Количество друзей: " + friendCount);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получении пользователя с наибольшим количеством друзей: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void getUserActions(Connection connection, int user_id) throws SQLException {
        String query = "SELECT 'post' AS action_type, id, text AS description, create_date AS action_date FROM posts WHERE author_id = ? " +
                "UNION " +
                "SELECT 'comment' AS action_type, id, text AS description, creation_date AS action_date FROM comments WHERE author_id = ? " +
                "UNION " +
                "SELECT 'like' AS action_type, id, 'Liked post ID ' || post_id AS description, (SELECT create_date FROM posts WHERE id = post_id) AS action_date FROM likes WHERE author_id = ? " +
                "UNION " +
                "SELECT 'sent message' AS action_type, id, text AS description, sent_date AS action_date FROM messages WHERE sender_id = ? " +
                "UNION " +
                "SELECT 'received message' AS action_type, id, text AS description, sent_date AS action_date FROM messages WHERE receiver_id = ? " +
                "UNION " +
                "SELECT 'friend' AS action_type, id, 'Became friends with user ID ' || friend_id AS description, null AS action_date FROM friends WHERE user_id = ? " +
                "UNION " +
                "SELECT 'friend' AS action_type, id, 'Became friends with user ID ' || user_id AS description, null AS action_date FROM friends WHERE friend_id = ? " +
                "ORDER BY action_date DESC";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, user_id);
            preparedStatement.setInt(2, user_id);
            preparedStatement.setInt(3, user_id);
            preparedStatement.setInt(4, user_id);
            preparedStatement.setInt(5, user_id);
            preparedStatement.setInt(6, user_id);
            preparedStatement.setInt(7, user_id);

            ResultSet rs = preparedStatement.executeQuery();
            System.out.println("Действия пользователя с ID: " + user_id);
            while (rs.next()) {
                String actionType = rs.getString("action_type");
                int actionId = rs.getInt("id");
                String description = rs.getString("description");
                Date actionDate = rs.getDate("action_date");
                System.out.println("Тип Действия: " + actionType + " | ID: " + actionId + " | Описание: " + description + " | Date: " + actionDate);
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при получении всех действий пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    //add
    private static void addUser(Connection connection, String first_name, String last_name, java.sql.Date birth_date, String email, String password) throws SQLException {
        resetUserIdSequenceIfTableEmpty(connection);
        String insertSQL = "INSERT INTO users (first_name, last_name, birth_date, email, password) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

            preparedStatement.setString(1, first_name);
            preparedStatement.setString(2, last_name);
            preparedStatement.setDate(3, birth_date);
            preparedStatement.setString(4, email);
            preparedStatement.setString(5, password);

            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Добавление прошло успешно!");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при добавлении пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void addPost(Connection connection, String text, int author_id) throws SQLException {
        resetPostIdSequenceIfTableEmpty(connection);
        if (userVerification(connection, author_id)) {
            System.out.println("Пользователь с id " + author_id + " не найден");
            return;
        }

        String insertSQL = "INSERT INTO posts (text, create_date, author_id) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            preparedStatement.setString(1, text);
            preparedStatement.setDate(2, NOW);
            preparedStatement.setInt(3, author_id);

            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Пост успешно добавлен!");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при добавлении поста: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void addComments(Connection connection, String text, int post_id, int author_id) throws SQLException {
        resetCommentIdSequenceIfTableEmpty(connection);
        if (postVerification(connection, post_id)) {
            System.out.println("Пост с ID: " + post_id + " не найден");
            return;
        }

        if (userVerification(connection, author_id)) {
            System.out.println("Пользователь с ID: " + author_id + " не найден");
            return;
        }

        String insertSQL = "INSERT INTO comments (text, creation_date, post_id, author_id) VALUES (?, ?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            preparedStatement.setString(1, text);
            preparedStatement.setDate(2, NOW);
            preparedStatement.setInt(3, post_id);
            preparedStatement.setInt(4, author_id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Комментарий успешно добавлен");
            } else {
                System.out.println("Комментарий не добавлен");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при добавлении комментариев: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void addLike(Connection connection, int post_id, int author_id) throws SQLException {
        resetLikeIdSequenceIfTableEmpty(connection);
        if (postVerification(connection, post_id)) {
            System.out.println("Поста с ID: " + post_id + " не найдено");
            return;
        }

        if (userVerification(connection, author_id)) {
            System.out.println("Пользователя с ID: " + author_id + " не найдено");
            return;
        }

        if (likeVerification(connection, author_id, post_id)) {
            System.out.println("Пост пользователя с ID: " + " уже поставлен");
            return;
        }

        String insertSQL = "INSERT INTO likes (post_id, author_id) VALUES (?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)){
            preparedStatement.setInt(1, post_id);
            preparedStatement.setInt(2, author_id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Лайк успешно поставлен");
            } else {
                System.out.println("Лайк не поставлен");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при добавлении лайка: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void addFriend(Connection connection, int user_id, int friend_id) throws SQLException {
        resetFriendIdSequenceIfTableEmpty(connection);
        if (user_id == friend_id) {
            System.out.println("Пользователь с ID: " + user_id + " не может добавить сам себя в друзья");
            return;
        }

        if (userVerification(connection, user_id) || userVerification(connection, friend_id)) {
            System.out.println("Пользователь с таким ID не найден");
            return;
        }

        if (friendVerification(connection, user_id, friend_id)) {
            System.out.println("Пользователи с ID: " + user_id + " и с ID: " + friend_id + " уже друзья");
            return;
        }

        String insertSQL = "INSERT INTO friends (user_id, friend_id) VALUES (?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            preparedStatement.setInt(1, user_id);
            preparedStatement.setInt(2, friend_id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Пользователь успешно добавлен в друзья");
            } else {
                System.out.println("Пользователь не добавлен в друзья");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при добавлении друга: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void sendMessage(Connection connection, String text, int sender_id, int receiver_id) throws SQLException {
        resetMessageIdSequenceIfTableEmpty(connection);
        if (userVerification(connection, sender_id) || userVerification(connection, receiver_id)) {
            System.out.println("Не существует пользователя с таким ID");
            return;
        }
        String insertMessageSQL = "INSERT INTO messages (text, sent_date, sender_id, receiver_id) VALUES (?, ?, ?, ?)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertMessageSQL)) {
            preparedStatement.setInt(3, sender_id);
            preparedStatement.setInt(4, receiver_id);
            preparedStatement.setDate(2, NOW);
            preparedStatement.setString(1, text);

            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Сообщение отправлено успешно!");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при отправке сообщения: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    //delete
    private static void deleteUser(Connection connection, int id) throws SQLException {
        if (userVerification(connection, id)) {
            System.out.println("Пользователя с ID: " + id + " не найдено");
            return;
        }
        String deleteSQL = "DELETE FROM users WHERE id = ?";

        try(PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL)) {
            preparedStatement.setInt(1, id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Пользователь успешно удален!");
            } else {
                System.out.println("Пользователь не удален");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteComment(Connection connection, int user_id, int comments_id) throws SQLException {
        if (userVerification(connection, user_id)) {
            System.out.println("Пользователь с ID: " + user_id + " не найдено");
            return;
        }
        if (commentVerification(connection, comments_id)) {
            System.out.println("Комментарий с ID: " + comments_id + " не найден");
            return;
        }
        String query = "DELETE FROM comments WHERE id = ? AND author_id = ?";

        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, comments_id);
            preparedStatement.setInt(2, user_id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Комментарий успешно удален");
            } else {
                System.out.println("Комментарий не удален");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении комментария: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteUserCascade(Connection connection, int user_id) throws SQLException {
        String deleteLikes = "DELETE FROM likes WHERE author_id = ?";
        String deleteComments = "DELETE FROM comments WHERE author_id = ?";
        String deleteMessages = "DELETE FROM messages WHERE sender_id = ? OR receiver_id = ?";
        String deletePosts = "DELETE FROM posts WHERE author_id = ?";
        String deleteFriends = "DELETE FROM friends WHERE user_id = ? OR friend_id = ?";
        String deleteUsers = "DELETE FROM users WHERE id = ?";

        connection.setAutoCommit(false);
        try {
            try (PreparedStatement preparedStatement = connection.prepareStatement(deleteLikes)) {
                preparedStatement.setInt(1, user_id);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Ошибка при удалении лайков: " + e.getMessage());
                throw e;
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(deleteComments)) {
                preparedStatement.setInt(1, user_id);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Ошибка при удалении комментариев: " + e.getMessage());
                throw e;
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(deleteMessages)) {
                preparedStatement.setInt(1, user_id);
                preparedStatement.setInt(2, user_id);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Ошибка при удалении сообщений: " + e.getMessage());
                throw e;
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(deletePosts)) {
                preparedStatement.setInt(1, user_id);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Ошибка при удалении постов: " + e.getMessage());
                throw e;
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(deleteFriends)) {
                preparedStatement.setInt(1, user_id);
                preparedStatement.setInt(2, user_id);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Ошибка при удалении друзей: " + e.getMessage());
                throw e;
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(deleteUsers)) {
                preparedStatement.setInt(1, user_id);
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                System.out.println("Ошибка при удалении пользователя: " + e.getMessage());
                throw e;
            }

            connection.commit();
            System.out.println("Пользователь и все связанные с ним данные успешно удалены");
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
        System.out.println();
    }

    private static void deleteLike(Connection connection, int post_id, int author_id) throws SQLException {
        if (userVerification(connection, author_id)) {
            System.out.println("Пользователя с ID: " + author_id + "не найдено");
            return;
        }

        if (postVerification(connection, post_id)) {
            System.out.println("Поста с ID: " + post_id + " не найдено");
            return;
        }

        String query = "DELETE FROM likes WHERE post_id = ? AND author_id = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, post_id);
            preparedStatement.setInt(2, author_id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Лайк успешно удален");
            } else {
                System.out.println("Лайк не удален");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении лайка: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteFriend(Connection connection, int user_id, int friend_id) throws SQLException {

        if (userVerification(connection, user_id) || userVerification(connection, friend_id)) {
            System.out.println("Пользователя с таким ID не найдено");
            return;
        }

        if (!friendVerification(connection, user_id, friend_id)) {
            System.out.println("Пользователи не друзья");
            return;
        }

        String query = "DELETE FROM friends WHERE user_id = ? AND friend_id = ?";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, user_id);
            preparedStatement.setInt(2, friend_id);
            int rowsAffected = preparedStatement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Пользователь успешно удален из друзей");
            } else {
                System.out.println("Пользователь не удален из друзей");
            }
            System.out.println();
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении пользователя из друзей: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteAllPosts(Connection connection, int id) throws SQLException {
        String query = "DELETE FROM posts WHERE author_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            int rowsAffected = statement.executeUpdate();
            System.out.println("Удалено постов: " + rowsAffected + " пользователя: " + id);
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении постов пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteAllLikes(Connection connection, int id) throws SQLException {
        String query = "DELETE FROM likes WHERE author_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            int rowsAffected = statement.executeUpdate();
            System.out.println("Удалено лайков: " + rowsAffected + " пользователя: " + id);
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении лайков пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteAllFriendsByUser(Connection connection, int id) throws SQLException {
        String query = "DELETE FROM friends WHERE user_id = ? OR friend_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            statement.setInt(2, id);
            int rowsAffected = statement.executeUpdate();
            System.out.println("Удалено друзей: " + rowsAffected +" пользователя: " + id);
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении друзей пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteAllMessagesByUser(Connection connection, int id) throws SQLException {
        String query = "DELETE FROM messages WHERE sender_id = ? OR receiver_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            statement.setInt(2, id);
            int rowsAffected = statement.executeUpdate();
            System.out.println("Удалено сообщений: " + rowsAffected + " пользователя: " + id);
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении сообщений пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void deleteAllCommentsByUser(Connection connection, int id) throws SQLException {
        String query = "DELETE FROM comments WHERE author_id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            int rowsAffected = statement.executeUpdate();
            System.out.println("Удалено комментариев: " + rowsAffected + " пользователя: " + id);
        } catch (SQLException e) {
            System.out.println("Ошибка при удалении комментариев пользователя: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    //update
    private static void updateUser(Connection connection, int id, String first_name, String last_name, java.sql.Date birth_date, String email, String password) throws SQLException {

        PreparedStatement statement = connection.prepareStatement("UPDATE users SET first_name = ?, last_name = ?, birth_date = ?, email = ?, password = ? WHERE id = ?");
        statement.setString(1, first_name);
        statement.setString(2, last_name);
        statement.setDate(3, birth_date);
        statement.setString(4, email);
        statement.setString(5, password);
        statement.setInt(6, id);

        int rowsAffected = statement.executeUpdate();
        if (rowsAffected > 0) {
            System.out.println("Пользователь успешно обновлен!");
            System.out.println();
            getUsers(connection);
        } else {
            System.out.println("Ошибка при обновлении пользователя");
        }
        System.out.println();
    }

    private static void updatePost(Connection connection, int author_id, int post_id, String text) throws SQLException {
        if (userVerification(connection, author_id)) {
            System.out.println("Пользователя с ID: " + author_id + " не существует");
            return;
        }
        if (postVerification(connection, post_id)) {
            System.out.println("Поста с ID: " + post_id + " не существует");
            return;
        }

        String query = "UPDATE posts SET text = ? WHERE id = ? AND author_id = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, text);
            statement.setInt(2, post_id);
            statement.setInt(3, author_id);

            int rowsAffected = statement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Пост успешно обновлен");
            } else {
                System.out.println("Не удалось обновить пост");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при обновлении поста: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    private static void updateComment(Connection connection, String text, int author_id, int comment_id) throws SQLException {
        if (userVerification(connection, author_id)) {
            System.out.println("Пользователя с ID: " + author_id + " не найден");
            return;
        }
        if (commentVerification(connection, comment_id)) {
            System.out.println("Комментарий с ID: " + comment_id + " не найден");
            return;
        }

        String query = "UPDATE comments SET text = ? WHERE id = ? AND author_id = ?";

        try(PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, text);
            statement.setInt(2, comment_id);
            statement.setInt(3, author_id);

            int rowsAffected = statement.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("Комментарий успешно обновлен");
            } else {
                System.out.println("Комментарий не удалось обновить");
            }
        } catch (SQLException e) {
            System.out.println("Ошибка при обновлении комментария: " + e.getMessage());
            throw e;
        }
        System.out.println();
    }

    /*Проверки*/
    private static boolean userVerification(Connection connection, int id) throws SQLException {
        String query = "SELECT 1 FROM users WHERE id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, id);
            ResultSet rs = preparedStatement.executeQuery();
            return !rs.next();
        } catch (SQLException e) {
            System.out.println("Ошибка при проверке на существовании пользователя: " + e.getMessage());
            throw e;
        }
    }

    private static boolean postVerification(Connection connection, int post_id) throws SQLException {
        String query = "SELECT 1 FROM posts WHERE id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, post_id);
            ResultSet rs = preparedStatement.executeQuery();
            return !rs.next();
        } catch (SQLException e) {
            System.out.println("Ошибка при проверки поста на существовании поста: " + e.getMessage());
            throw e;
        }
    }

    private static boolean friendVerification(Connection connection, int user_id, int friend_id) throws SQLException {
        String query = "SELECT 1 FROM friends WHERE user_id = ? AND friend_id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, user_id);
            preparedStatement.setInt(2, friend_id);
            ResultSet rs = preparedStatement.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            System.out.println("Ошибка при проверки друзей: " + e.getMessage());
            throw e;
        }
    }

    private static boolean commentVerification(Connection connection, int comment_id) throws SQLException {
        String query = "SELECT 1 FROM comments WHERE id = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, comment_id);
            ResultSet rs = preparedStatement.executeQuery();
            return !rs.next();
        } catch (SQLException e) {
            System.out.println("Ошибка при проверки комментария: " + e.getMessage());
            throw e;
        }
    }

    private static boolean likeVerification(Connection connection, int user_id, int post_id) throws SQLException {
        String query = "SELECT 1 FROM likes WHERE post_id = ? AND author_id = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, post_id);
            preparedStatement.setInt(2, user_id);
            ResultSet rs = preparedStatement.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            System.out.println("Ошибка при проверки лайка: " + e.getMessage());
            throw e;
        }
    }
}