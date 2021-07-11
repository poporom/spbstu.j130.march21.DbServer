/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jdbc;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 *
 * @author roman
 */
public class DbServer implements IDbService {

    public static final String URL = "jdbc:derby://localhost:1527/test";
    private String query;

    private Connection con;

    public static void main(String[] args) throws DocumentException {

        boolean b;

        DbServer demo = new DbServer();

        demo.checkDriver();
        demo.setConnection("test", "test");

        Authors ar1 = new Authors(2, "Tom Hawkins", "new author");
        Authors ar2 = new Authors(5, "Tom Hawkins", "");
        Authors ar3 = new Authors(5, "", "update");

        //b = demo.deleteAuthor(ar1);
        //b = demo.deleteAuthor(5); 
        //b = demo.addAuthor(ar1);
        //b = demo.addAuthor(ar2);
        //b = demo.addAuthor(ar3);
        Documents doc1 = new Documents(2, "First report", "Second content", 2);

        //b = demo.addDocument(doc1,ar1);
        //Documents[] docsByAuthor = demo.findDocumentByAuthor(ar1);
        //Documents[] docsByContent = demo.findDocumentByContent("Second");
        //demo.closeConnection();
    }

    private void checkDriver() {

        Enumeration<Driver> e = DriverManager.getDrivers();
        while (e.hasMoreElements()) {
            Driver d = e.nextElement();
            System.out.println(d.getClass().getCanonicalName());
        }
        try {
            Driver d = DriverManager.getDriver(URL);
            if (d != null) {
                System.out.println("Ok");
            }
        } catch (SQLException ex) {
            System.out.println("Error #1: " + ex.getMessage());
        }
    }

    private void setConnection(String user, String psw) {
        try {
            con = DriverManager.getConnection(URL, user, psw);
            System.out.println("Connection is opened...");
        } catch (SQLException ex) {
            System.out.println("Error #2: " + ex.getMessage());
        }
    }

    private void closeConnection() {
        try {
            if (con != null && !con.isClosed()) {
                con.close();
                System.out.println("Connection is closed.");
            }
        } catch (SQLException ex) {
            System.out.println("Error #3: " + ex.getMessage());
        }
    }

    private boolean deleteAuthorById(int id) throws DocumentException {

        int count = 0;

        query = "delete\n"
                + "from documents\n"
                + "where author_id = ?";

        try (PreparedStatement ps = con.prepareStatement(query);) {
            ps.setInt(1, id);
            ps.executeUpdate();

        } catch (SQLException ex) {
            throw new DocumentException("SQLException: " + ex.getMessage());
        }

        query = "delete\n"
                + "from authors\n"
                + "where id = ?";

        try (PreparedStatement ps = con.prepareStatement(query)) {

            ps.setInt(1, id);
            count = ps.executeUpdate();

        } catch (SQLException ex) {
            throw new DocumentException("SQLException: " + ex.getMessage());
        }

        if (count > 0) {
            return true;
        } else {
            return false;
        }

    }

    private Documents[] findDocumentByAuthorOrContent(Authors author, String content) throws DocumentException {

        ArrayList<Documents> data = new ArrayList<Documents>();
        query = "select * \n"
                + "from documents\n"
                + "where " + ((author == null) ? "locate(?, doc_text) > 0" : "author_id = ?");

        try (PreparedStatement ps = con.prepareStatement(query)) {

            if (author == null) {
                ps.setString(1, content);
            } else {
                ps.setInt(1, author.getAuthor_id());
            }

            ps.execute();

            try (ResultSet rs = ps.getResultSet()) {

                while (rs.next()) {
                    data.add(new Documents(rs.getInt(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getDate(4),
                            rs.getInt(5)));

                }
            }

        } catch (SQLException ex) {
            throw new DocumentException("SQLException: " + ex.getMessage());
        }

        if (data.size() == 0) {
            return null;
        } else {
            return data.toArray(new Documents[data.size()]);
        }

    }

    @Override
    public boolean addAuthor(Authors author) throws DocumentException {

        if (author.getAuthor_id() <= 0) {

            int max_id = 0;

            query = "select max(id)\n"
                    + "from authors";

            try (Statement st = con.createStatement()) {

                try (ResultSet rs = st.executeQuery(query)) {
                    rs.next();
                    max_id = rs.getInt(1);

                }

            } catch (SQLException ex) {
                throw new DocumentException("SQLException: " + ex.getMessage());

            }

            query = "insert into authors\n"
                    + "values (?,?,?)";

            try (PreparedStatement ps = con.prepareStatement(query)) {

                ps.setInt(1, max_id + 1);
                ps.setString(2, author.getAuthor());
                ps.setString(3, author.getNotes());
                ps.execute();

                return true;

            } catch (SQLException ex) {
                throw new DocumentException("SQLException: " + ex.getMessage());

            }

        } else {

            query = "update authors\n"
                    + "set notes = ?\n"
                    + "where id = ?";

            try (PreparedStatement ps = con.prepareStatement(query)) {

                ps.setString(1, author.getNotes());
                ps.setInt(2, author.getAuthor_id());
                ps.execute();

                return false;

            } catch (SQLException ex) {
                throw new DocumentException("SQLException: " + ex.getMessage());

            }

        }

    }

    @Override
    public boolean addDocument(Documents doc, Authors author) throws DocumentException {

        if (doc.getDocument_id() <= 0) {

            addAuthor(author);

            int max_id = 0;

            query = "select max(id)\n"
                    + "from documents";

            try (Statement st = con.createStatement()) {

                try (ResultSet rs = st.executeQuery(query)) {
                    rs.next();
                    max_id = rs.getInt(1);

                }

            } catch (SQLException ex) {
                throw new DocumentException("SQLException: " + ex.getMessage());

            }

            query = "insert into documents\n"
                    + "values (?,?,?,?,?)";

            try (PreparedStatement ps = con.prepareStatement(query)) {

                ps.setInt(1, max_id + 1);
                ps.setString(2, doc.getTitle());
                ps.setString(3, doc.getText());
                ps.setDate(4, new java.sql.Date(doc.getDate().getTime()));
                ps.setInt(5, author.getAuthor_id());
                ps.execute();

                return true;

            } catch (SQLException ex) {
                throw new DocumentException("SQLException: " + ex.getMessage());

            }

        } else {

            addAuthor(author);

            query = "update documents\n"
                    + "set doc_name = ?, doc_text = ?, oc_date = ?, author_id = ?\n"
                    + "where id = ?";

            try (PreparedStatement ps = con.prepareStatement(query)) {

                ps.setString(1, doc.getTitle());
                ps.setString(2, doc.getText());
                ps.setDate(3, new java.sql.Date(doc.getDate().getTime()));
                ps.setInt(4, author.getAuthor_id());
                ps.setInt(5, doc.getDocument_id());
                ps.execute();

                return false;

            } catch (SQLException ex) {
                throw new DocumentException("SQLException: " + ex.getMessage());

            }

        }

    }

    @Override
    public Documents[] findDocumentByAuthor(Authors author) throws DocumentException {
        return findDocumentByAuthorOrContent(author, "");
    }

    @Override
    public Documents[] findDocumentByContent(String content) throws DocumentException {
        return findDocumentByAuthorOrContent(null, content);
    }

    @Override
    public boolean deleteAuthor(Authors author) throws DocumentException {
        return deleteAuthorById(author.getAuthor_id());
    }

    @Override
    public boolean deleteAuthor(int id) throws DocumentException {
        return deleteAuthorById(id);
    }

    @Override
    public void close() {
        closeConnection();
    }

}
