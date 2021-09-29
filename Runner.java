import db.Queries;

public class Runner {
    public static void main(String[] args) {
        Queries q = new Queries();

        q.printCityTeamsCount();
        q.printMaxPayedCoach();
        q.printMaxPayedPlayerPerSeason();
        q.printTeamsTotalPointsPerSeason(1400);
        q.printMaxGoaledDerbyPerSeason(1400);
        q.printChampTeamPerSeason(1400);
    }
}
