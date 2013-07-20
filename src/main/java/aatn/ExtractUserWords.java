package aatn;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 5:49 PM
 */
public class ExtractUserWords extends BaseFunction {
  private static final Logger LOG = LoggerFactory.getLogger(ExtractUserWords.class);
  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    String gnipEventXmlStr = tuple.getString(0);
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      InputSource is = new InputSource(new StringReader(gnipEventXmlStr));

      Document doc = builder.parse(is);

      String userId = null;
      NodeList authors = doc.getElementsByTagName("author");
      for (int i = 0; authors != null && i < authors.getLength(); i++) {
        Node author = authors.item(i);
        NodeList authorElements = author.getChildNodes();
        for (int j = 0; authorElements != null && j < authorElements.getLength(); j++) {
          Node authorElement = authorElements.item(j);
          String name = authorElement.getNodeName();
          if ("id".equals(name)) {
            userId = authorElement.getChildNodes().item(0).getNodeValue();
          }
        }
      }

      if (StringUtils.isNotEmpty(userId)) {
        String content = null;
        NodeList contents = doc.getElementsByTagName("object");
        for (int i = 0; contents != null && i < contents.getLength(); i++) {
          Node contentItem = contents.item(i);
          NodeList contentElements = contentItem.getChildNodes();
          for (int j = 0; contentElements != null && j < contentElements.getLength(); j++) {
            Node contentElement = contentElements.item(j);
            String name = contentElement.getNodeName();
            if ("content".equals(name)) {
              content = contentElement.getChildNodes().item(0).getNodeValue();
            }
          }
        }

        if (StringUtils.isNotEmpty(content)) {
          for (String word : content.split("\\s")) {
            collector.emit(
                ImmutableList.<Object>of(userId, word)
            );
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Failed reading input stream", e);
    } catch (SAXException e) {
      LOG.error("Failed parsing input stream", e);
    } catch (ParserConfigurationException e) {
      LOG.error("Failed parsing input stream", e);
    }
  }
}

