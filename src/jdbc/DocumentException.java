/*
 * DEV-J130. Задача №2.
 */

package jdbc;

/**
 * Класс представляет общее исключение, возникающее при работе с документами.
 * @author (C)Y.D.Zakovryashin, 12.11.2020
 */
public class DocumentException extends Exception {

    public DocumentException() {
    }

    public DocumentException(String string) {
        super(string);
    }
    
}