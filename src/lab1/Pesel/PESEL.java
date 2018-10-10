package lab1.Pesel;

public class PESEL {
    private String pesel;

    public PESEL(String _pesel){
        pesel=_pesel;
    }



    public static boolean check(String tocheck) {
        if (tocheck.length() != 11) {
            System.out.println("Nieprawidłowa ilość cyfr");
            return false;
        }


        for (int i = 0; i < tocheck.length(); i++) {
            if ((48 <= tocheck.charAt(i)) && (tocheck.charAt(i) <= 57)) {
            } else {
                System.out.println("Pesel może zawierać tylko cyfry");
                return false;
            }
        }


        int[] checknumbers = {9, 7, 3, 1, 9, 7, 3, 1, 9, 7};
        int sum = 0;

        for (int j = 0; j < tocheck.length()-1; j++) {
            checknumbers[j]= checknumbers[j] * (tocheck.charAt(j)-'0');
            sum+=checknumbers[j];
        }


        if (sum % 10 == 0) {
        } else {
            System.out.println("Zła liczba kontrolna");
            return false;
        }

        System.out.println("Twój pesel jest poprawny!");
        return true;

    }
}

