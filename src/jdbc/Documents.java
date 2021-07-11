/*
 * DEV-J130. Задача №2.
 */
package jdbc;

import java.util.Date;
import java.util.Objects;

/**
 * Класс представляет общие сведения о документе и его содержание. Таблица
 * Documents имеет следующую структуру: - идентификатор документа,
 * представленный целым числом, является первичным ключом таблицы; - поле
 * названия документа длиной до 64 символов включительно, которое не может быть
 * пустым; - поле с основным текстом документа длиной до 1024 символов
 * включительно; - поле даты создания документа, которое должно быть обязательно
 * заполнено; - поле ссылки на автора документа, которое является внешним
 * ключом, ссылающимся на первичный ключ таблицы Authors, и которое также не
 * может быть пустым.
 *
 * @author (C)Y.D.Zakovryashin, 12.11.2020
 */
public class Documents {

    public static final int VERSION = 267384;
    private final int document_id;
    private String title;
    private String text;
    private Date date;
    private int author_id;

    public Documents(int document_id, String title, String text, int author_id) {
        this(document_id, title, text,
                new Date(System.currentTimeMillis()), author_id);
    }

    public Documents(int document_id, String title, String text, Date date, int author_id) {
        this.document_id = document_id;
        this.title = title;
        this.text = text;
        this.date = date;
        this.author_id = author_id;
    }

    public int getDocument_id() {
        return document_id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getAuthor_id() {
        return author_id;
    }

    public void setAuthor_id(int author_id) {
        this.author_id = author_id;
    }

    @Override
    public int hashCode() {
        return VERSION + this.document_id + Objects.hashCode(this.title) + this.author_id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Documents other = (Documents) obj;
        return !(this.document_id != other.document_id
                || this.author_id != other.author_id
                || !Objects.equals(this.title, other.title));
    }

}