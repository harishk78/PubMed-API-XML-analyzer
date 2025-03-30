package a1;

import javax.xml.stream.*;
import javax.xml.stream.events.XMLEvent;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class search {

    private static final String API_KEY = "f04417fac4ac3911b46dd8ed8118179bdb08"; //API key allows more requests to be sent
    private static final int BATCH_SIZE = 10; // Number of requests per batch
    private static final int THREAD_POOL_SIZE = 5; // Number of threads for concurrent processing
    private static final int REQUESTS_PER_SECOND = 10; // API rate limit, pubmed with a key allows 10 reqs per second
    private static final int MAX_RETRIES = 3; // The number of retries allowed when

    //Maps the article titles to the appropriate PMIDs
    private static final Map<String, List<String>> extractedResults = new HashMap<>();

    public static void main(String[] args) throws Exception {
        try {

            String filePath = "4020a1-datasets.xml"; // Input XML file
            String outputFilePath = "group1_results.xml"; // Output XML file

            ArrayList<String> articleTitles = new ArrayList<>();

            //For parsing the provided XML file and getting the titles
            XMLInputFactory factory = XMLInputFactory.newInstance();
            XMLStreamReader reader = factory.createXMLStreamReader(new FileInputStream(filePath));

            boolean inArticleTitle = false;

            //while loop for extracting the article titles and loading them into an ArrayList
            while (reader.hasNext()) {

                //for moving through the xml event
                int event = reader.next();

                switch (event) {
                    //for when when the tag <articleTitle> is met the boolean var is turned true
                    case XMLStreamConstants.START_ELEMENT:
                        if (reader.getLocalName().equals("ArticleTitle")) {
                            inArticleTitle = true;
                        }
                        break;

                    //If there is text, it will check if the boolean value is true and proceed to add the text into a string
                    case XMLStreamConstants.CHARACTERS:
                        if (inArticleTitle) {
                            String title = reader.getText().trim();
                            //if title string is not empty it gets add to the article titles List
                            if (!title.isEmpty()) {
                                articleTitles.add(title);
                            }
                            //set to false once that is done
                            inArticleTitle = false;
                        }
                        break;

                    //reaches the end of the element and then sets it to false
                    case XMLStreamConstants.END_ELEMENT:
                        if (reader.getLocalName().equals("ArticleTitle")) {
                            inArticleTitle = false;
                        }
                        break;
                }
            }

            //Encodes the article titles so that they are properly formatted to be put into a URL
            ArrayList<String> encodedTitles = new ArrayList<>(); //array for encoded titles

            for (String title : articleTitles) {
                title = cleanTitle(title); //clean title method responsible for getting rid of formatting errors in articles starting with "re: "
                title = title.replace("“", "\"").replace("”", "\""); //get rid of any curly braces that are not correctly formatted
                String encodedTitle = URLEncoder.encode(title, StandardCharsets.UTF_8.toString()); //transform the titles into utf-8 encoder
                encodedTitles.add(encodedTitle);
            }


            ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE); //executor services handles batch processing
            List<Future<String>> futures = new ArrayList<>();   //for sending tasks to the executor service

            //lists number of titles to process
            int totalTitles = encodedTitles.size();
            System.out.println("Total titles to process: " + totalTitles);

            //this code block processes the titles in batches
            for (int i = 0; i < totalTitles; i += BATCH_SIZE) { //iterate through the batches

                int end = Math.min(i + BATCH_SIZE, totalTitles); //end is set so that it doesn't exceed the number of titles

                /*this gets the current batch of titles that need to be processed using a sublist and creates
                *another one with the original article titles for when they need to be extracted to the XML
                */
                List<String> batch = encodedTitles.subList(i, end);
                List<String> batchTitles = articleTitles.subList(i, end);

                System.out.println("Processing batch: " + (i / BATCH_SIZE + 1) + " of " + (totalTitles / BATCH_SIZE + 1));

                for (int j = 0; j < batch.size(); j++) { //iterate through the titles in the batch to be submitted for processing
                    String encodedTitle = batch.get(j);
                    String articleTitle = batchTitles.get(j);

                    //set the API url
                    String apiUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=%22" + encodedTitle + "%22[Title:~3]&api_key=" + API_KEY;

                    //sends URL to the API response handler
                    futures.add(executor.submit(() -> {
                        String xmlResponse = getApiResponseWithRetry(apiUrl, MAX_RETRIES);
                        parseXml(xmlResponse, articleTitle); //method finding PMID
                        return xmlResponse;
                    }));
                }

                //iterates through the list of future tasks submitted to the executorService
                for (Future<String> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        System.err.println("Error processing API response: " + e.getMessage());
                    }
                }
                futures.clear();



                //for the API rate limit
                if (i + BATCH_SIZE < totalTitles) {
                    Thread.sleep(1000 / REQUESTS_PER_SECOND * BATCH_SIZE);
                }
            }


            executor.shutdown(); // Shutdown the thread pool
            System.out.println("Processing complete.");

            // Export extracted results to XML
            exportResultsToXml(outputFilePath);
            System.out.println("Extracted results have been saved to: " + outputFilePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //cleans title by getting rid of the quotation marks that are in "reply" articles
    private static String cleanTitle(String title) {
        //if the title starts with Re: and ends in a quotation, remove the quotations
        if (title.startsWith("Re: \"") && title.endsWith("\"")) {
            title = "Re: " + title.substring(5, title.length() - 1).trim();
        }
        return title;
    }

    //Method for handling 429 rate limiting errors
    private static String getApiResponseWithRetry(String apiUrl, int maxRetries) throws IOException {
        int retryCount = 0;
        while (retryCount < maxRetries) {
            try {
                return getApiResponse(apiUrl); //calls get Api response method
            } catch (IOException e) {   //catch if API fails
                if (e.getMessage().contains("429")) {   //if message contains 429 print error
                    retryCount++;
                    System.out.println("Rate limit exceeded. Retrying (" + retryCount + "/" + maxRetries + ")...");
                    try {
                        Thread.sleep(1000); // 1 second delay before retrying
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Thread interrupted during retry delay.", ie);
                    }
                } else {
                    throw e; //if its not a 429 error
                }
            }
        }
        throw new IOException("Failed to get API response after " + maxRetries + " retries.");
    }

    //this method sends a get request to the API
    private static String getApiResponse(String apiUrl) throws IOException {

        //set up HTTP connection
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        //checks for a 429 error
        int responseCode = connection.getResponseCode();
        if (responseCode == 429) {
            throw new IOException("429 Too Many Requests");
        }


        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = in.readLine()) != null) {
            response.append(line);
        }
        in.close();

        return response.toString();
    }

    //parses through the XML file and finds the PMID that matches the article title
    private static void parseXml(String xml, String articleTitle) throws XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLEventReader eventReader = factory.createXMLEventReader(new StringReader(xml));

        List<String> pmids = new ArrayList<>(); // List to store extracted PMIDs

        //goes through the XML event
        while (eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();

            //if the tag is Id then go to it
            if (event.isStartElement() && event.asStartElement().getName().getLocalPart().equals("Id")) {
                event = eventReader.nextEvent();
                //if there is text then the PMID is extracted and added to the arrayList
                if (event.isCharacters()) {
                    String pmid = event.asCharacters().getData();
                    pmids.add(pmid);
                    System.out.println("Extracted ID: " + pmid);
                }
            }
        }

        // Store the extracted PMIDs with their article titles
        if (!pmids.isEmpty()) {
            extractedResults.put(articleTitle, pmids);
        }
    }

    //Exports the extracted results to an XML file.
    private static void exportResultsToXml(String filePath) throws IOException {
        StringBuilder xmlBuilder = new StringBuilder();

        //adds the top header in the XML file
        xmlBuilder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xmlBuilder.append("<PubmedArticleSet>\n");

        //iterates and builds the XML structure
        for (Map.Entry<String, List<String>> entry : extractedResults.entrySet()) {
            String title = entry.getKey();
            List<String> pmids = entry.getValue();

            xmlBuilder.append("  <PubmedArticle>\n");
            for (String pmid : pmids) {
                xmlBuilder.append("    <PMID>").append(pmid).append("</PMID>\n");
            }
            xmlBuilder.append("    <ArticleTitle>").append(title).append("</ArticleTitle>\n");
            xmlBuilder.append("  </PubmedArticle>\n");
        }

        xmlBuilder.append("</PubmedArticleSet>");

        // Write the XML to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(xmlBuilder.toString());
        }
    }
}