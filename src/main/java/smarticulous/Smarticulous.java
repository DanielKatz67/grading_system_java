package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Submission;
import smarticulous.db.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The Smarticulous class, implementing a grading system.
 */
public class Smarticulous {

    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the {@link Smarticulous} SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables, creating them if necessary:
     *
     * <table>
     *   <caption><em>Table name: <strong>User</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>UserId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Username</td><td>Text</td></tr>
     *   <tr><td>Firstname</td><td>Text</td></tr>
     *   <tr><td>Lastname</td><td>Text</td></tr>
     *   <tr><td>Password</td><td>Text</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Exercise</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>DueDate</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Question</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>Desc</td><td>Text</td></tr>
     *   <tr><td>Points</td><td>Integer</td></tr>
     * </table>
     * In this table the combination of ExerciseId and QuestionId together comprise the primary key.
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Submission</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>UserId</td><td>Integer</td></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>SubmissionTime</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>QuestionGrade</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Grade</td><td>Real</td></tr>
     * </table>
     * In this table the combination of SubmissionId and QuestionId together comprise the primary key.
     *
     * @param dburl The JDBC url of the database to open (will be of the form "jdbc:sqlite:...")
     * @return the new connection
     * @throws SQLException
     */
    public Connection openDB(String dburl) throws SQLException {
        db = DriverManager.getConnection(dburl);
        Statement st = db.createStatement();
        st.executeUpdate("CREATE TABLE IF NOT EXISTS User          (UserId INTEGER PRIMARY KEY, Username TEXT UNIQUE, Firstname TEXT, Lastname TEXT, Password TEXT);");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Exercise      (ExerciseId INTEGER PRIMARY KEY, Name TEXT, DueDate INTEGER);");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Question      (ExerciseId INTEGER, QuestionId INTEGER, Name TEXT, Desc TEXT, Points INTEGER, PRIMARY KEY (ExerciseId, QuestionId));");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Submission    (SubmissionId INTEGER PRIMARY KEY, UserId INTEGER, ExerciseId INTEGER, SubmissionTime INTEGER);");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS QuestionGrade (SubmissionId INTEGER, QuestionId INTEGER, Grade REAL, PRIMARY KEY (SubmissionId, QuestionId));");

        return db;
    }

    /**
     * Close the DB if it is open.
     *
     * @throws SQLException
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist.
     * If a user with user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @param password
     * @return the userid.
     * @throws SQLException
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        // Selecting the user with the same userName (unique), if exits
        PreparedStatement psSelect = db.prepareStatement("SELECT UserId FROM User WHERE UserName=?");
        psSelect.setString(1, user.username);
        ResultSet res = psSelect.executeQuery();

        // The user exits - need to update
        if(res.next()){
            PreparedStatement psUpdate = db.prepareStatement("UPDATE User SET Firstname=?, Lastname=?, Password=? WHERE UserName=? ");
            psUpdate.setString(1, user.firstname);
            psUpdate.setString(2, user.lastname);
            psUpdate.setString(3, password);
            psUpdate.setString(4, user.username);
            psUpdate.executeUpdate();
            return res.getInt("UserId");
        }
        // The user does not exit - need to add
        else{
            PreparedStatement psInsert = db.prepareStatement("INSERT INTO User (Username, Firstname, Lastname, Password) VALUES (?,?,?,?) ;");
            psInsert.setString(1, user.username);
            psInsert.setString(2, user.firstname);
            psInsert.setString(3, user.lastname);
            psInsert.setString(4, password);
            psInsert.executeUpdate();

            // Retrieve the generated key; this is the UserId
            ResultSet keys = psInsert.getGeneratedKeys();
            if (keys.next()) {
                // The index of the generated key is 1
                return keys.getInt(1);
            } else {
                throw new SQLException("Creating user failed, no ID obtained.");
            }
        }
    }


    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches; false otherwise.
     * @throws SQLException
     * <p>
     * Note: this is totally insecure. For real-life password checking, it's important to store only
     * a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        PreparedStatement ps = db.prepareStatement("SELECT Password FROM User WHERE UserName=?");
        ps.setString(1, username);
        ResultSet res = ps.executeQuery();

        // The user exits - need to check the password
        if(res.next() &&  password.equals(res.getString("Password"))){
            return true;
        }
        // The user does not exit or the password is wrong
        else{
            return false;
        }
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     * @throws SQLException
     */
    public int addExercise(Exercise exercise) throws SQLException {
        PreparedStatement psSelect = db.prepareStatement("SELECT * FROM Exercise WHERE ExerciseId=?");
        psSelect.setInt(1, exercise.id);
        ResultSet res = psSelect.executeQuery();

        // An exercise with this id already existed in the database
        if(res.next()){
            return -1 ;
        }
        // New exercise
        else{
            PreparedStatement psInsert = db.prepareStatement("INSERT INTO Exercise (Name, DueDate, ExerciseId) VALUES (?,?,?);");
            psInsert.setString(1, exercise.name);
            // converting the Date to milliseconds since 1/1/1970 as needed
            java.sql.Date dueDateMillis = new java.sql.Date(exercise.dueDate.getTime());
            psInsert.setDate(2, dueDateMillis);
            psInsert.setInt(3, exercise.id);
            psInsert.executeUpdate();

            // adding the questions to the DB
            for(Exercise.Question question : exercise.questions){
                addQuestion(question, exercise.id);
            }

            return exercise.id;
        }
    }

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     * @throws SQLException
     */
    private void addQuestion(Exercise.Question question, int exerciseId) throws SQLException {
        PreparedStatement psInsert = db.prepareStatement("INSERT INTO Question (ExerciseId, Name, Desc, Points) VALUES (?,?,?,?);");
        psInsert.setInt(1, exerciseId);
        psInsert.setString(2, question.name);
        psInsert.setString(3, question.desc);
        psInsert.setInt(4, question.points);
        psInsert.executeUpdate();
    }


    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     *
     * @return list of all exercises.
     * @throws SQLException
     */
    public List<Exercise> loadExercises() throws SQLException {
        PreparedStatement psAllExercises = db.prepareStatement("SELECT * FROM Exercise ORDER BY ExerciseId;");
        ResultSet resExercises = psAllExercises.executeQuery();

        List<Exercise> exercisesList = new ArrayList<>();
        int ExerciseId, pointsQ;
        String NameEx, nameQ, descQ;
        Date DueDate;

        while (resExercises.next()){
            ExerciseId = resExercises.getInt("ExerciseId");
            NameEx = resExercises.getString("Name");
            DueDate = resExercises.getDate("DueDate");
            Exercise currentEx = new Exercise(ExerciseId, NameEx, DueDate);

            PreparedStatement psQuestions = db.prepareStatement("SELECT * FROM Question WHERE ExerciseId=?;");
            psQuestions.setInt(1, ExerciseId);
            ResultSet resQuestions = psQuestions.executeQuery();

            // Store all the questions of this exercise
            List<Exercise.Question> Questions = new ArrayList<>();
            while (resQuestions.next()){
                nameQ = resQuestions.getString("Name");
                descQ = resQuestions.getString("Desc");
                pointsQ = resQuestions.getInt("Points");
                currentEx.addQuestion(nameQ, descQ, pointsQ);
            }

            // Add the current exercise to the list
            exercisesList.add(currentEx);
        }
        return exercisesList;
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     * @throws SQLException
     */
    public int storeSubmission(Submission submission) throws SQLException {
        // TODO: Implement
        return -1;
    }


    // ============= Submission Query ===============


    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     *
     */
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @param stmt
     * @return
     * @throws SQLException
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     * @throws SQLException
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }


    /**
     * Return the submission with the highest total grade
     *
     * @param user the user for which we retrieve the best submission
     * @param exercise the exercise for which we retrieve the best submission
     * @return
     * @throws SQLException
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}
