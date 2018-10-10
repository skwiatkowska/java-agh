package lab1.Pesel;

public class PESELProgram {
    public static void main(String [] argv){
        System.out.print("Type your Pesel: ");
        String pesel = "12345678912";
        PESEL peseltocheck=new PESEL(pesel);

        peseltocheck.check(pesel);


    }
}
