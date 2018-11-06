--4.4
--1
select left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", left(rtrim(z.sztuk || '-' || c.nazwa || ' - ' || c.opis),50) as "Zawartość"
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
order by 1;
--2
select p.idpudelka as "Id", left(rtrim(p.nazwa ||' - '|| p.opis),45) as "Pudelko", 
left(rtrim(z.sztuk || '-' || c.nazwa || ' - ' || c.opis),45) as "Zawartość"
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where p.idpudelka = 'heav'
order by 2;
--3
select left(rtrim(p.nazwa ||' - '|| p.opis),45) as "Pudelko", left(rtrim(z.sztuk || '-' || c.nazwa || ' - ' || c.opis),45) as "Zawartość"
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where p.nazwa like '%Kolekcja%'
order by 2;
 
--4.5 jezeli odpalic bez dodatkowych kolumn potwierdzajacych wynik to jest git inaczej sie powtarza
--1
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),45) as "Pudelko", p.cena, c.idczekoladki
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where c.idczekoladki like 'd09'
order by 1;
--2
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),45) as "Pudelko", p.cena, c.nazwa
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where c.nazwa like 'S%'
order by 1;
--3
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena, left(rtrim(z.sztuk || '-' || c.nazwa),45) as "Zawartość"
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where z.sztuk > 3
order by 1;
--4
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena, c.nadzienie
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where c.nadzienie like 'truskawki'
order by 1;
--5
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
except
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where c.czekolada like 'gorzka'
order by 1;
--6
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena, left(rtrim(z.sztuk || '-' || c.nazwa),45) as "Zawartość"
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where z.sztuk > 2 and c.nazwa like 'Gorzka truskawkowa'
order by 1;
--7
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
except
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where c.orzechy is not null
order by 1;
--9
select distinct left(rtrim(p.nazwa ||' - '|| p.opis),50) as "Pudelko", p.cena, c.nadzienie
from pudelka p natural join zawartosc z join czekoladki c using(idczekoladki)
where c.nadzienie is null
order by 1;

--4.6
--1
select c1.idczekoladki as "ID1", c1.nazwa, c1.koszt as "Koszt1", c2.idczekoladki as "ID2", c2.koszt as "Koszt2"
from czekoladki c1, czekoladki c2
where c2.idczekoladki like 'd08' and c1.koszt > c2.koszt;
--2
select distinct k2.nazwa
from klienci k1 natural join zamowienia z1 natural join artykuly a1,
klienci k2 natural join zamowienia z2 natural join artykuly a2
where k1.nazwa like 'Górka Alicja' and a1.idpudelka = a2.idpudelka
order by 1;
--3
select distinct k1.miejscowosc, k2.nazwa,(k2.ulica || ' ' || k2.miejscowosc || ' ' || k2.kod) as "Adres"
from klienci k1 natural join zamowienia z1 natural join artykuly a1,
klienci k2 natural join zamowienia z2 natural join artykuly a2
where k1.miejscowosc like 'Katowice' and a1.idpudelka = a2.idpudelka
order by 1;
--or
select distinct k1.nazwa, k1.miejscowosc, (k1.ulica || ' ' || k1.miejscowosc || ' ' || k1.kod) as "Adres" 
from klienci k1 join klienci k2 on (k1.idklienta<>k2.idklienta) join zamowienia z on (z.idklienta=k1.idklienta) 
join artykuly a using (idzamowienia) join pudelka p using (idpudelka) 
where k2.miejscowosc='Katowice' and k1.miejscowosc!=k2.miejscowosc;
