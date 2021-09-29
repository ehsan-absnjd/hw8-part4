package db;

import configs.DbConfig;
import org.mariadb.jdbc.MariaDbDataSource;

import java.sql.*;

public class Queries {
    private MariaDbDataSource dataSource = new MariaDbDataSource();
    private Connection connection;

    public Queries(){
        try {
            dataSource.setPassword(DbConfig.PASSWORD);
            dataSource.setUrl(DbConfig.URL);
            dataSource.setUser(DbConfig.USERNAME);
            connection = dataSource.getConnection();
            init();
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void init(){
        final String CREATE_CITY_TABLE="CREATE TABLE IF NOT EXISTS city(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50), PRIMARY KEY(id) )";
        final String CREATE_TEAM_TABLE="CREATE TABLE IF NOT EXISTS team(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50) , city_id INT,PRIMARY KEY(id), FOREIGN KEY(city_id) REFERENCES city(id))";
        final String CREATE_STADIUM_TABLE="CREATE TABLE IF NOT EXISTS stadium(id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50), city_id INT, PRIMARY KEY(id), FOREIGN KEY(city_id) REFERENCES city(id))";
        final String CREATE_PLAYER_TABLE="CREATE TABLE IF NOT EXISTS player( id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50),PRIMARY KEY(id))";
        final String CREATE_COACH_TABLE="CREATE TABLE IF NOT EXISTS coach( id INT NOT NULL AUTO_INCREMENT, name VARCHAR(50),PRIMARY KEY(id))";
        final String CREATE_TEAMSEASON_TABLE="CREATE TABLE IF NOT EXISTS team_season(id INT NOT NULL AUTO_INCREMENT,season INT, team_id INT, FOREIGN KEY(team_id) REFERENCES team(id), PRIMARY KEY(id) )";
        final String CREATE_COACHSEASON_TABLE="CREATE TABLE IF NOT EXISTS coach_season (id INT NOT NULL AUTO_INCREMENT, season INT, coach_id  INT, salary BIGINT, team_season_id INT, FOREIGN KEY(team_season_id) REFERENCES team_season(id) ,FOREIGN KEY(coach_id) REFERENCES coach(id), PRIMARY KEY(id))";
        final String CREATE_PLAYERSEASON_TABLE="CREATE TABLE IF NOT EXISTS player_season(id INT NOT NULL AUTO_INCREMENT, season INT, player_id INT, salary BIGINT, team_season_id INT, FOREIGN KEY(team_season_id) REFERENCES team_season(id) ,FOREIGN KEY(player_id) REFERENCES player(id), PRIMARY KEY(id))";
        final String CREATE_MATCHZ_TABLE="CREATE TABLE IF NOT EXISTS matchz(id INT NOT NULL AUTO_INCREMENT, guest_goals INT, host_goals INT, guest_points INT, host_points INT, stadium_id INT,guest_team_season_id INT, host_team_season_id INT, PRIMARY KEY(id), FOREIGN KEY(stadium_id) REFERENCES stadium(id),FOREIGN KEY(guest_team_season_id) REFERENCES team_season(id) , FOREIGN KEY (host_team_season_id) REFERENCES team_season(id))";
        final String CREATE_MATCHZPLAYER_TABLE="CREATE TABLE IF NOT EXISTS matchz_player(matchz_id INT, player_season_id INT, FOREIGN KEY(matchz_id) REFERENCES matchz(id), FOREIGN KEY(player_season_id) REFERENCES player_season(id) , PRIMARY KEY(matchz_id, player_season_id))";
        final String CREATE_MATCHZGOAL_TABLE="CREATE TABLE IF NOT EXISTS matchz_goal(matchz_id INT, player_season_id INT,goal_count INT, FOREIGN KEY(matchz_id) REFERENCES matchz(id), FOREIGN KEY(player_season_id) REFERENCES player_season(id) , PRIMARY KEY(matchz_id, player_season_id))";
        executeWithoutResult(CREATE_CITY_TABLE);
        executeWithoutResult(CREATE_TEAM_TABLE);
        executeWithoutResult(CREATE_STADIUM_TABLE);
        executeWithoutResult(CREATE_PLAYER_TABLE);
        executeWithoutResult(CREATE_COACH_TABLE);
        executeWithoutResult(CREATE_TEAMSEASON_TABLE);
        executeWithoutResult(CREATE_COACHSEASON_TABLE);
        executeWithoutResult(CREATE_PLAYERSEASON_TABLE);
        executeWithoutResult(CREATE_MATCHZ_TABLE);
        executeWithoutResult(CREATE_MATCHZPLAYER_TABLE);
        executeWithoutResult(CREATE_MATCHZGOAL_TABLE);
    }
    public void printMaxPayedCoach() {
        final String QUERY = "SELECT c.name , cs .salary FROM coach_season cs JOIN coach c " +
                "ON cs.coach_id = c.id WHERE cs.salary = (SELECT MAX(salary) FROM coach_season)";
        ResultSet resultSet = getResultsetForQuery(QUERY);
        try{
            while(resultSet!=null && resultSet.next()){
                String name = resultSet.getString(1);
                long salary = resultSet.getLong(2);
                System.out.println("all time max payed coach is : "+name + " with salary : " + salary);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void printMaxPayedPlayerPerSeason(){
        final String QUERY = "SELECT p.name , ps.salary , ps.season FROM player_season ps JOIN" +
                " player p ON ps.player_id =p.id JOIN (SELECT MAX(salary) AS salary , season from player_season ps" +
                " GROUP BY season ) e ON ps.season = e.season AND ps.salary =e.salary";
        ResultSet resultSet = getResultsetForQuery(QUERY);
        try{
            while(resultSet!=null && resultSet.next()){
                String name = resultSet.getString(1);
                long salary = resultSet.getLong(2);
                int season = resultSet.getInt(3);
                System.out.println("in season:" + season + " max payed player was : "+ name + " with salary : "+salary );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void printCityTeamsCount(){
        final String QUERY = "SELECT city.name , COUNT(*) FROM city JOIN team ON team.city_id =city.id " +
                "GROUP BY city.id";
        ResultSet resultSet = getResultsetForQuery(QUERY);
        try{
            while(resultSet!=null && resultSet.next()){
                String city = resultSet.getString(1);
                int count = resultSet.getInt(2);
                System.out.println("city: " + city + " has " + count + " teams" );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void printTeamsTotalPointsPerSeason(int season){
        final String QUERY = "SELECT t.name , SUM(e.points) AS total_points FROM " +
                "(SELECT m.id AS match_id, m.guest_points AS points , ts.team_id AS team_id , " +
                "ts.season AS season FROM matchz m JOIN team_season ts ON m.guest_team_season_id =ts.id  " +
                "UNION SELECT m.id AS match_id,m.host_points AS points , ts.team_id AS team_id , ts.season AS season " +
                "FROM matchz m JOIN team_season ts ON m.host_team_season_id=ts.id) e JOIN team t ON e.team_id = t.id WHERE " +
                "e.season=? GROUP BY t.name ";
        ResultSet resultSet = getResultsetForPreparedQuery(QUERY, season,1);
        try{
            while(resultSet!=null && resultSet.next()){
                String team = resultSet.getString(1);
                int points = resultSet.getInt(2);
                System.out.println("team : " + team + " has " + points + " points in season : " + season );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void printChampTeamPerSeason(int season){
        final String QUERY = "SELECT e.name AS name , e.total_points AS total_points FROM (select t.name , sum(e.points) AS total_points FROM " +
                "(SELECT m.id AS match_id, m.guest_points AS points , ts.team_id AS team_id , ts.season  AS season FROM " +
                "matchz m JOIN team_season ts ON m.guest_team_season_id =ts.id  UNION SELECT m.id AS match_id,m.host_points AS points ," +
                "ts.team_id AS team_id , ts.season AS season FROM matchz m JOIN team_season ts ON m.host_team_season_id  =ts.id) e JOIN " +
                "team t ON e.team_id = t.id WHERE e.season=? GROUP BY t.name) e WHERE e.total_points = (SELECT MAX(total_points) FROM " +
                "(SELECT t.name , SUM(e.points) AS total_points FROM (select m.id AS match_id, m.guest_points AS points , ts.team_id AS team_id , " +
                "ts.season  AS season FROM matchz m JOIN team_season ts ON m.guest_team_season_id =ts.id  UNION SELECT m.id AS match_id, " +
                "m.host_points AS points ,ts.team_id AS team_id , ts.season AS season FROM matchz m JOIN team_season ts ON " +
                "m.host_team_season_id =ts.id) e JOIN team t ON e.team_id = t.id WHERE e.season=? GROUP BY t.name) e)";
        ResultSet resultSet = getResultsetForPreparedQuery(QUERY,season,2);
        try{
            while(resultSet!=null && resultSet.next()){
                String name = resultSet.getString(1);
                int points = resultSet.getInt(2);
                System.out.println("the champion is " + name+ " with " + points + " points in season "+ season);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void printMaxGoaledDerbyPerSeason(int season){
        final String QUERY = "SELECT e.* FROM (SELECT m.id AS matchs_id, thost.name AS host_team , tguest.name AS guest_team , " +
                "m.guest_goals + m.host_goals AS goals FROM matchz m JOIN team_season tshost ON m.host_team_season_id = tshost.id " +
                "JOIN team thost ON tshost.team_id =thost.id JOIN team_season tsguest ON m.guest_team_season_id = tsguest.id JOIN " +
                "team tguest ON tsguest.team_id =tguest.id WHERE tguest.city_id =thost.city_id AND tshost.season =?) e WHERE " +
                "e.goals = (SELECT MAX(f.goals) FROM (SELECT thost.name AS host_team , tguest.name AS guest_team , " +
                "m.guest_goals + m.host_goals AS goals FROM matchz m JOIN team_season tshost ON m.host_team_season_id = tshost.id JOIN" +
                " team thost ON tshost.team_id =thost.id JOIN team_season tsguest ON m.guest_team_season_id = tsguest.id JOIN team tguest " +
                "ON tsguest.team_id =tguest.id WHERE tguest.city_id =thost.city_id AND tshost.season =?) f)";
        ResultSet resultSet = getResultsetForPreparedQuery(QUERY,season,2);
        try{
            while(resultSet!=null && resultSet.next()){
                int matchNum = resultSet.getInt(1);
                String hostTeam = resultSet.getString(2);
                String guestTeam = resultSet.getString(3);
                int goals = resultSet.getInt(4);
                System.out.println("derby with max goals is match number " + matchNum + " with team " + hostTeam + " as host and " + guestTeam +" as guest with total goals of " + goals);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private ResultSet getResultsetForQuery(String query) {
        ResultSet resultSet=null;
        try {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }
    private ResultSet getResultsetForPreparedQuery(String query , int year, int count) {
        ResultSet resultSet=null;
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            for(int i=1; i<=count ;i++)
                statement.setInt(i , year);
            resultSet = statement.executeQuery();
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }
    private void  executeWithoutResult(String query) {
        ResultSet resultSet=null;
        try {
            Statement statement = connection.createStatement();
            statement.execute(query);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
