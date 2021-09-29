import db.Queries;

public class Runner {
    public static void main(String[] args) {
        Queries q = new Queries();

        q.printCityTeamsCount();
        q.printMaxPayedCoach();
        q.printMaxPayedPlayerPerSeason();
        q.printTeamsTotalPointsBySeason(1400);
        q.printMaxGoaledDerbyBySeason(1400);
        q.printChampTeamBySeason(1400);
    }
}
