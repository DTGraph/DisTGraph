//package windows;
//
//import java.awt.BorderLayout;
//import java.awt.GridLayout;
//import java.awt.event.ActionEvent;
//import java.awt.event.ActionListener;
//import java.awt.event.ItemEvent;
//import java.awt.event.ItemListener;
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.UnsupportedEncodingException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//import javax.swing.ButtonGroup;
//import javax.swing.JButton;
//import javax.swing.JFrame;
//import javax.swing.JLabel;
//import javax.swing.JPanel;
//import javax.swing.JRadioButton;
//import javax.swing.JScrollPane;
//import javax.swing.JTextArea;
//import javax.swing.JTextField;
//
//import Summary.Summary;
//import home.tut.yake.Yake;
//import home.tut.yake.Yake.DedupAlg;
//import home.tut.yake.Yake.KeywordExtractorOutput;
//
//public class display2 extends JFrame{
//
//	JFrame frame = new JFrame("�������۵Ķ��ı�ժҪ");// ���崰�����
//
//	JButton text_label = new JButton("�ı����ݣ�");
//	JTextArea input = new JTextArea();
//	JScrollPane input_text = new JScrollPane(input);
//
//	JButton title_label = new JButton("�������ݣ�");
//	JTextArea input_title = new JTextArea();
//
//	JButton keyword_label = new JButton("�ؼ���");
//	JTextArea keyword = new JTextArea();
//
//	JButton summary_label = new JButton("����ժҪ");
//	JTextArea output = new JTextArea();
//	JScrollPane output_text = new JScrollPane(output);
//
//	ButtonGroup group_summary = new ButtonGroup();
//	JRadioButton[] summary_num = {new JRadioButton("3��"), new JRadioButton("4��"), new JRadioButton("5��"),
//								  new JRadioButton("6��"), new JRadioButton("7��"), new JRadioButton("8��"),
//								  new JRadioButton("9��"), new JRadioButton("10��")};
//
//	ButtonGroup group_keyword = new ButtonGroup();
//	JRadioButton[] keyword_num = {new JRadioButton("10��"), new JRadioButton("20��"), new JRadioButton("30��"),
//			  new JRadioButton("40��"), new JRadioButton("50��")};
//
//	//Ĭ��ѡȡժҪ��
//	int select_num = 5;
//	//Ĭ��ѡȡ�ؼ��ʸ���
//	int choice_num = 20;
//
//	public display2() throws IOException { //���췽bai��
//
//		text_label.setBounds(10, 10, 120, 20);
//		input_text.setBounds(10, 40, 1480, 400);
//		input_text.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
//
//		title_label.setBounds(470, 10, 120, 20);
//		input_title.setBounds(600, 10, 800, 20);
//
//		keyword_label.setBounds(10, 450, 120, 20);
//		keyword.setBounds(10, 480, 1450, 30);
//
//		summary_label.setBounds(10, 520, 120, 20);
//		output_text.setBounds(10, 550, 1480, 200);
//		output_text.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
//
//		for(int i = 0; i < summary_num.length; i ++) {
//			summary_num[i].setBounds(200 + i * 75, 520, 60, 20);
//			group_summary.add(summary_num[i]);
//		}
//
//		for(int i = 0; i < keyword_num.length; i ++) {
//			keyword_num[i].setBounds(200 + i * 75, 450, 60, 20);
//			group_keyword.add(keyword_num[i]);
//		}
//
//		frame.setLayout(null);
//		frame.setSize(1510, 800);
//		frame.setLocation(25, 20);
//		frame.setVisible(true);
//
//		frame.add(text_label);
//		frame.add(input_text);
//
//		frame.add(title_label);
//		frame.add(input_title);
//
//		frame.add(keyword_label);
//		frame.add(keyword);
//
//		frame.add(summary_label);
//		frame.add(output_text);
//
//		for(JRadioButton choice : summary_num)
//			frame.add(choice);
//		for(JRadioButton choice : keyword_num)
//			frame.add(choice);
//
//		//Ԥ����
//		Summary summary = new Summary("Wiki300.txt");
//
//		//��input.txt�л�ȡ�ı�����
//		text_label.addActionListener(
//				new ActionListener(){
//					public void actionPerformed(ActionEvent e) {
//						String inputPath = "D:\\part-r\\input.txt";
//				    	String charset = "UTF-8";
//
//				    	try {
//							BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath), charset));
//							String muti_text = "";
//							String line = null;
//					      	while((line = reader.readLine()) != null) {
//					      		muti_text += line + "\n";
//					      	}
//					      	reader.close();
//					      	input.setText(muti_text);
//
//						} catch (UnsupportedEncodingException e1) {
//							// TODO Auto-generated catch block
//							e1.printStackTrace();
//						} catch (FileNotFoundException e1) {
//							// TODO Auto-generated catch block
//							e1.printStackTrace();
//						} catch (IOException e1) {
//							// TODO Auto-generated catch block
//							e1.printStackTrace();
//						}
//					}
//				});
//
//		//���ı������л�ȡ�ı�����
//		title_label.addActionListener(
//				new ActionListener(){
//					public void actionPerformed(ActionEvent e) {
//						String text = input.getText();
//
//						String[] split = text.split("\\n");
//						for(String line : split) {
//					  		String item[] = line.split(" ");
//
//					  		if(item[3].equals("ԭ��")) {
//					  			String title = "";
//						  		for(int i = 4; i < item.length; i ++)
//						  			title += item[i] + " ";
//					  			input_title.setText(title);
//					  			break;
//					  		}
//
//						}
//					}
//				});
//
//
//		//ѡ�����ɵĹؼ�������
//		for(int i = 0; i < keyword_num.length; i ++) {
//			keyword_num[i].addItemListener(
//					new ItemListener() {
//					public void itemStateChanged(ItemEvent e) {
//						if(keyword_num[0].isSelected())
//							choice_num = 10;
//						else if(keyword_num[1].isSelected())
//							choice_num = 20;
//						else if(keyword_num[2].isSelected())
//							choice_num = 30;
//						else if(keyword_num[3].isSelected())
//							choice_num = 40;
//						else
//							choice_num = 50;
//					}
//				});
//		}
//
//		//���ı������л�ȡ�ؼ���
////		keyword_label.addActionListener(
////				new ActionListener(){
////					public void actionPerformed(ActionEvent e) {
////						String muti_text = input.getText();
////						String title = input_title.getText();
////
////						String total_text = "";
////						List<String> doc = new ArrayList<>();
////
////
////						String[] split = muti_text.split("\\n");
////						for(String line : split) {
////							String text = "";
////					  		String item[] = line.split(" ");
////
////					  		for(int i = 4; i < item.length; i ++)
////					  			text += item[i] + " ";
////
////					  		doc.add(text + "��");
////					  		total_text += text + "��";
////						}
////
////						long t1 = System.currentTimeMillis();
////						List<String> list_keywords = summary.KeySet(total_text, title, choice_num);
////						long t2 = System.currentTimeMillis();
////						System.out.println("All Time: " + (t2 - t1) + "\n");
////
////						String res = "";
////						for(String  s : list_keywords)
////							res += s + " ";
////						keyword.setText(res);
////					}
////				});
//
//		//ѡ�����ɵ�ժҪ��������
//		for(int i = 0; i < summary_num.length; i ++) {
//			summary_num[i].addItemListener(
//					new ItemListener() {
//					public void itemStateChanged(ItemEvent e) {
//						if(summary_num[0].isSelected())
//							select_num = 3;
//						else if(summary_num[1].isSelected())
//							select_num = 4;
//						else if(summary_num[2].isSelected())
//							select_num = 5;
//						else if(summary_num[3].isSelected())
//							select_num = 6;
//						else if(summary_num[4].isSelected())
//							select_num = 7;
//						else if(summary_num[5].isSelected())
//							select_num = 8;
//						else if(summary_num[6].isSelected())
//							select_num = 9;
//						else
//							select_num = 10;
//					}
//				});
//		}
//
//		//���ı�ժҪ�㷨
//		summary_label.addActionListener(
//				new ActionListener(){
//					public void actionPerformed(ActionEvent e) {
//						String text = input.getText();
//						String title = input_title.getText();
//
//						try {
//							String res = run(text, title, summary, select_num);
//							output.setText(res);
//
//						} catch (IOException e1) {
//							// TODO Auto-generated catch block
//							e1.printStackTrace();
//						}
//
//					}
//				});
//
//	}
//
//	public String run(String muti_text, String title, Summary summary, int select_num) throws IOException{
//		List<String> doc = new ArrayList<>();
//
//      	List<String> author = new ArrayList<>();
//      	String main = "";
//
//		String[] split = muti_text.split("\\n");
//		for(String line : split) {
//			String text = "";
//	  		String item[] = line.split(" ");
//
//	  		for(int i = 4; i < item.length; i ++)
//	  			text += item[i] + " ";
//
//	  		author.add(item[2]);
//      		if(item[3].equals("ԭ��"))
//      			main = item[2];
//
//	  		doc.add(text + "��");
//		}
//
//		long t1 = System.currentTimeMillis();
//		String res = "";//summary.MyMethod_Muti_Summary(doc, title, author, main, select_num);
//		System.out.println(res);
//		long t2 = System.currentTimeMillis();
//		System.out.println("All Time: " + (t2 - t1) + "\n");
//
//		return res;
//	}
//
//	public static void main (String args[]) throws IOException{
//
//		new display2();
//	}
//}
//
//
