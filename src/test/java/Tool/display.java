package Tool;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import java.io.IOException;
import java.util.*;
import java.util.List;

import javax.swing.*;

import DBExceptions.TransactionException;
import Element.RelationshipAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import config.DTGConstants;
import performance.DTG.simpleTest;
import tool.MemMvcc.DTGSortedList;
import tool.MemMvcc.MVCCObject;

public class display extends JFrame{

	JFrame frame = new JFrame("道路交通数据演示");// 定义窗体对象

	JLabel title = new JLabel("道路信息管理系统", JLabel.CENTER);

	JLabel importData = new JLabel("                      数据导入:");

	JButton importTopoButton = new JButton("添加拓扑图数据");
	JButton importTempData = new JButton("导入时态图数据");

	JLabel readDataLable = new JLabel("输入时间获取道路信息      请输入时间:");
	JTextField TimeField = new JTextField(16);
	JButton readDataButton = new JButton("读取道路信息");

	JLabel maxLable = new JLabel("max value");
	JTextField textMaxField = new JTextField(10);
	JLabel minLable = new JLabel("按车辆数筛选道路：   min value");
	JTextField textMinField = new JTextField(10);
	JLabel minTimeLable = new JLabel("    时间");
	JTextField textMinTimeField = new JTextField(10);
	JButton readDataBetweenButton = new JButton("确定");

    JButton mvccbButton = new JButton("内存MVCC演示");
	JButton StoreInfo = new JButton("查看集群状态");
	JButton simpleChange = new JButton("简单操作");

	JLabel roadInfo = new JLabel("道路信息：");

	final JTextArea jta= new JTextArea();
	//在文本框上添加滚动条
	final JScrollPane jsp = new JScrollPane(jta);

    Font f = new Font(Font.DIALOG,Font.BOLD,16);
    Font f2 = new Font(Font.DIALOG,Font.BOLD,17);
	Font f3 = new Font(Font.DIALOG,Font.BOLD,30);


	private ImageIcon background;
	private JPanel imagePanel;

	private static Map<Integer, String> regionMap = new HashMap<>();

	public display() throws IOException {
		int hight = 32;

		title.setBounds(0, 20, 900, 40);
		title.setFont(f3);
		frame.add(title);

		importData.setBounds(100, 80, 200, hight);
		importData.setFont(f);
		frame.add(importData);

		importTopoButton.setBounds(320, 80, 200, hight);
		importTopoButton.setFont(f);
		frame.add(importTopoButton);

		importTempData.setBounds(570, 80, 200, hight);
		importTempData.setFont(f);
		frame.add(importTempData);

		readDataLable.setBounds(150, 130, 350, hight);
		readDataLable.setFont(f2);
		frame.add(readDataLable);

		TimeField.setBounds(470, 130, 80, hight);
		TimeField.setFont(f2);
		frame.add(TimeField);

		readDataButton.setBounds(570, 130, 200, hight);
		readDataButton.setFont(f);
		frame.add(readDataButton);


		maxLable.setBounds(440, 180, 80, hight);
		maxLable.setFont(f2);
		frame.add(maxLable);

		textMaxField.setBounds(520, 180, 50, hight);
		textMaxField.setFont(f);
		frame.add(textMaxField);

		minLable.setBounds(100, 180, 260, hight);
		minLable.setFont(f2);
		frame.add(minLable);

		textMinField.setBounds(360, 180, 50, hight);
		textMinField.setFont(f);
		frame.add(textMinField);

		minTimeLable.setBounds(580, 180, 80, hight);
		minTimeLable.setFont(f2);
		frame.add(minTimeLable);

		textMinTimeField.setBounds(640, 180, 50, hight);
		textMinTimeField.setFont(f);
		frame.add(textMinTimeField);

		readDataBetweenButton.setBounds(700, 180, 100, hight);
		readDataBetweenButton.setFont(f);
		frame.add(readDataBetweenButton);

		roadInfo.setBounds(100, 215, 100, hight);
		roadInfo.setFont(f);
		frame.add(roadInfo);

		jta.setLineWrap(true);
		jta.setFont(f);
		jsp.setBounds(100, 250, 700, 400);
		//默认的设置是超过文本框才会显示滚动条，以下设置让滚动条一直显示
		jsp.setVerticalScrollBarPolicy( JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		frame.add(jsp);

		mvccbButton.setBounds(200, 680, 150, hight);
		mvccbButton.setFont(f);
		frame.add(mvccbButton);

		StoreInfo.setBounds(375, 680, 150, hight);
		StoreInfo.setFont(f);
		frame.add(StoreInfo);

		simpleChange.setBounds(550, 680, 150, hight);
		simpleChange.setFont(f);
		frame.add(simpleChange);


		frame.setLayout(null);
		frame.setSize(900, 800);
		frame.setLocation(250, 200);
		frame.setVisible(true);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		background = new ImageIcon("D:\\picture\\7.jpg");// 背景图片
		JLabel label = new JLabel(background);// 把背景图片显示在一个标签里面
		// 把标签的大小位置设置为图片刚好填充整个面板
		label.setBounds(0, 0, background.getIconWidth(),
				background.getIconHeight());
		// 把内容窗格转化为JPanel，否则不能用方法setOpaque()来使内容窗格透明
		imagePanel = (JPanel) frame.getContentPane();
		imagePanel.setOpaque(false);
		frame.getLayeredPane().setLayout(null);
		// 把背景图片添加到分层窗格的最底层作为背景
		frame.getLayeredPane().add(label, new Integer(Integer.MIN_VALUE));
		frame.setResizable(false);


		simpleTest test = new simpleTest();

		importTopoButton.addActionListener(
				new ActionListener(){
					public void actionPerformed(ActionEvent e) {
						try{
							importTopoAction();
						}catch (Exception ex){
							getStoreInfo();
						}
					}
				});

		mvccbButton.addActionListener(
                new ActionListener(){
                    public void actionPerformed(ActionEvent e) {
                        addFrame();
                    }
                });

		StoreInfo.addActionListener(
				new ActionListener(){
					public void actionPerformed(ActionEvent e) {
						getStoreInfo();
					}
				});

		importTempData.addActionListener(
				new ActionListener(){
					public void actionPerformed(ActionEvent e) {
						try{
							importTempDataAction();
						}catch (Exception ex){
							getStoreInfo();
						}
					}
				});

		readDataButton.addActionListener(
				new ActionListener(){
					public void actionPerformed(ActionEvent e) {
						try{
							readDataButtonAction();
						}catch (Exception ex){
							getStoreInfo();
						}
					}
				});


		readDataBetweenButton.addActionListener(
				new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						try{
							readDataBetweenAction();
						}catch (Exception ex){
							getStoreInfo();
						}
					}
				}
		);

		simpleChange.addActionListener(
				new ActionListener() {
					@Override
					public void actionPerformed(ActionEvent e) {
						try{
							simpleChange();
						}catch (Exception ex){
							getStoreInfo();
						}
					}
				}
		);

	}

	private void addFrame(){
        JFrame frame = new JFrame("道路信息管理系统");
        frame.setLayout(null);
        frame.setSize(800, 450);
        frame.setLocation(250, 250);
        frame.setVisible(true);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JLabel memLable = new JLabel("内存MVCC演示", JLabel.CENTER);
		memLable.setBounds(0, 30, 800, 40);
		memLable.setFont(f3);
		frame.add(memLable);

        background = new ImageIcon("D:\\picture\\7.jpg");// 背景图片
        JLabel label = new JLabel(background);// 把背景图片显示在一个标签里面
        // 把标签的大小位置设置为图片刚好填充整个面板
        label.setBounds(0, 0, background.getIconWidth(),
                background.getIconHeight());
        // 把内容窗格转化为JPanel，否则不能用方法setOpaque()来使内容窗格透明
        imagePanel = (JPanel) frame.getContentPane();
        imagePanel.setOpaque(false);
        frame.getLayeredPane().setLayout(null);
        // 把背景图片添加到分层窗格的最底层作为背景
        frame.getLayeredPane().add(label, new Integer(Integer.MIN_VALUE));
        frame.setResizable(false);

        JLabel readDataLable = new JLabel("输入查询事务版本号:");
        JTextField version = new JTextField(16);
        JButton preprocess_label = new JButton("查询数据");

        readDataLable.setBounds(200, 110, 350, 32);
        readDataLable.setFont(f2);
        frame.add(readDataLable);

        version.setBounds(380, 110, 80, 32);
        version.setFont(f2);
        frame.add(version);

        preprocess_label.setBounds(470, 110, 200, 32);
        preprocess_label.setFont(f);
        frame.add(preprocess_label);

        preprocess_label.addActionListener(
                new ActionListener(){
                    public void actionPerformed(ActionEvent e) {
                        String str = version.getText();
                        if(str.equals(""))
                        {
                            Object[] options = { "OK ", "CANCEL " };
                            JOptionPane.showOptionDialog(null, "您还没有输入版本信息 ", "提示", JOptionPane.DEFAULT_OPTION,
                                    JOptionPane.WARNING_MESSAGE,null, options, options[0]);
                        }
                        int time = Integer.parseInt(str);
                        String res = (String)findTest(time);
                        if(res == null){
                            Object[] options = { "OK ", "CANCEL " };
                            JOptionPane.showOptionDialog(null, "没有对应版本数据！", "提示", JOptionPane.DEFAULT_OPTION,
                                    JOptionPane.WARNING_MESSAGE,null, options, options[0]);
                        }else{
                            Object[] options = { "OK ", "CANCEL " };
                            JOptionPane.showOptionDialog(null, "获取到的数据为：" + res, "提示", JOptionPane.DEFAULT_OPTION,
                                    JOptionPane.WARNING_MESSAGE,null, options, options[0]);
                        }

                        return;
                    }
                });

        final JTextArea jta= new JTextArea();
        //在文本框上添加滚动条
        final JScrollPane jsp = new JScrollPane(jta);

        jta.setLineWrap(true);
        jta.setFont(f);
        jsp.setBounds(100, 170, 600, 200);
        //默认的设置是超过文本框才会显示滚动条，以下设置让滚动条一直显示
        jsp.setVerticalScrollBarPolicy( JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
        frame.add(jsp);

        Map<String, DTGSortedList> map = getTopoMemMVCC();
        jta.setText("             开始版本号       结束版本号         属性值\r\n");

        for(Map.Entry<Long, Node> entry : MvccMap.entrySet()){
            Node n = entry.getValue();
            String s = "                         " + n.start + "                     " + n.end + "                " + n.value + "\r\n";
            jta.append(s);
        }
    }

	private void simpleChange(){
		JFrame frame = new JFrame("道路信息管理系统");
		frame.setLayout(null);
		frame.setSize(800, 500);
		frame.setLocation(250, 250);
		frame.setVisible(true);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JLabel memLable = new JLabel("简单时态属性操作", JLabel.CENTER);
		memLable.setBounds(0, 30, 800, 40);
		memLable.setFont(f3);
		frame.add(memLable);

		background = new ImageIcon("D:\\picture\\7.jpg");// 背景图片
		JLabel label = new JLabel(background);// 把背景图片显示在一个标签里面
		// 把标签的大小位置设置为图片刚好填充整个面板
		label.setBounds(0, 0, background.getIconWidth(),
				background.getIconHeight());
		// 把内容窗格转化为JPanel，否则不能用方法setOpaque()来使内容窗格透明
		imagePanel = (JPanel) frame.getContentPane();
		imagePanel.setOpaque(false);
		frame.getLayeredPane().setLayout(null);
		// 把背景图片添加到分层窗格的最底层作为背景
		frame.getLayeredPane().add(label, new Integer(Integer.MIN_VALUE));
		frame.setResizable(false);

		JLabel readDataLable = new JLabel("修改时态属性： 道路id:");
		readDataLable.setBounds(150, 110, 350, 32);
		readDataLable.setFont(f2);
		frame.add(readDataLable);

		JTextField id = new JTextField(16);
		id.setBounds(350, 110, 80, 32);
		id.setFont(f2);
		frame.add(id);

		JLabel readDataLable2 = new JLabel("   属性名:");
		readDataLable2.setBounds(450, 110, 80, 32);
		readDataLable2.setFont(f2);
		frame.add(readDataLable2);

		JTextField proName = new JTextField(16);
		proName.setBounds(550, 110, 80, 32);
		proName.setFont(f2);
		frame.add(proName);

		JButton change = new JButton("修改");
		change.setBounds(300, 190, 200, 32);
		change.setFont(f);
		frame.add(change);

		JLabel readDataLable3 = new JLabel("开始时间:");
		readDataLable3.setBounds(100, 150, 80, 32);
		readDataLable3.setFont(f2);
		frame.add(readDataLable3);

		JTextField start = new JTextField(16);
		start.setBounds(200, 150, 80, 32);
		start.setFont(f2);
		frame.add(start);

		JLabel readDataLable4 = new JLabel("结束时间:");
		readDataLable4.setBounds(300, 150, 80, 32);
		readDataLable4.setFont(f2);
		frame.add(readDataLable4);

		JTextField end = new JTextField(16);
		end.setBounds(400, 150, 80, 32);
		end.setFont(f2);
		frame.add(end);

		JLabel readDataLable5 = new JLabel("  属性值:");
		readDataLable5.setBounds(500, 150, 80, 32);
		readDataLable5.setFont(f2);
		frame.add(readDataLable5);

		JTextField value = new JTextField(16);
		value.setBounds(600, 150, 80, 32);
		value.setFont(f2);
		frame.add(value);


		change.addActionListener(
				new ActionListener(){
					public void actionPerformed(ActionEvent e) {
						try{
							String str = id.getText();//获取输入内容
							String str2 = proName.getText();//获取输入内容
							String str3 = start.getText();
							String str4 = end.getText();
							String str5 = value.getText();
							//判断是否输入了
							if(str.equals(""))
							{
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入id ", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}else if(str2.equals("")){
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入属性名称", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}else if(str3.equals("")){
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入开始时间", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}
							else if(str4.equals("")){
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入结束时间", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}else if(str5.equals("")){
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入属性值", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}
							simpleChangeAction(Integer.parseInt(str), str2, Integer.parseInt(str3), Integer.parseInt(str4), str5);
						}catch (Exception ex){
							Object[] options = { "OK ", "CANCEL " };
							JOptionPane.showOptionDialog(null, "输入有误或者修改失败", "提示", JOptionPane.DEFAULT_OPTION,
									JOptionPane.WARNING_MESSAGE,null, options, options[0]);
						}

					}
				});


		JLabel readDataLable6 = new JLabel("读取时态属性： 道路id:");
		readDataLable6.setBounds(150, 270, 350, 32);
		readDataLable6.setFont(f2);
		frame.add(readDataLable6);

		JTextField id2 = new JTextField(16);
		id2.setBounds(350, 270, 80, 32);
		id2.setFont(f2);
		frame.add(id2);

		JLabel readDataLable7 = new JLabel("   属性名:");
		readDataLable7.setBounds(450, 270, 80, 32);
		readDataLable7.setFont(f2);
		frame.add(readDataLable7);

		JTextField proName2 = new JTextField(16);
		proName2.setBounds(550, 270, 80, 32);
		proName2.setFont(f2);
		frame.add(proName2);

		JButton read = new JButton("读取");
		read.setBounds(370, 320, 200, 32);
		read.setFont(f);
		frame.add(read);

		JLabel readDataLable8 = new JLabel("时间:");
		readDataLable8.setBounds(220, 320, 50, 32);
		readDataLable8.setFont(f2);
		frame.add(readDataLable8);

		JTextField time2 = new JTextField(16);
		time2.setBounds(270, 320, 80, 32);
		time2.setFont(f2);
		frame.add(time2);


		read.addActionListener(
				new ActionListener(){
					public void actionPerformed(ActionEvent e) {
						try{
							String str = id2.getText();//获取输入内容
							String str2 = proName2.getText();//获取输入内容
							String str3 = time2.getText();
							//判断是否输入了
							if(str.equals(""))
							{
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入id ", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}else if(str2.equals("")){
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入属性名称", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}else if(str3.equals("")){
								Object[] options = { "OK ", "CANCEL " };
								JOptionPane.showOptionDialog(null, "您还没有输入时间", "提示", JOptionPane.DEFAULT_OPTION,
										JOptionPane.WARNING_MESSAGE,null, options, options[0]);
								return;
							}
							String res = (String)simpleReadAction(Integer.parseInt(str), str2, Integer.parseInt(str3));
							Object[] options = { "OK ", "CANCEL " };
							JOptionPane.showOptionDialog(null, "结果是：" + res, "提示", JOptionPane.DEFAULT_OPTION,
									JOptionPane.WARNING_MESSAGE,null, options, options[0]);
						}catch (Exception ex){
							Object[] options = { "OK ", "CANCEL " };
							JOptionPane.showOptionDialog(null, "输入有误或者修改失败", "提示", JOptionPane.DEFAULT_OPTION,
									JOptionPane.WARNING_MESSAGE,null, options, options[0]);
						}

					}
				});
	}

    Map<Long, Node> MvccMap = new HashMap<>();

    public Object findTest(int version){
        try{
            Map<String, DTGSortedList> map = getTopoMemMVCC();
            DTGSortedList list = map.get("name");
            MVCCObject object = list.findData(version);
            return object.getValue();
        }catch (Exception e){
            return null;
        }

    }

    private Map<String, DTGSortedList> getTopoMemMVCC(){
        MvccMap = new HashMap<>();
        Map<String, DTGSortedList> map = new HashMap<>();
        DTGSortedList list1;

        list1 = new DTGSortedList();
        MVCCObject o = new MVCCObject(1, "version-1");
        list1.insert(o);
        addToMvccMap(o);
        MVCCObject o1 = new MVCCObject(3, "version-3");
        list1.insert(o1);
        addToMvccMap(o1);
        MVCCObject o2 = new MVCCObject(5, "version-5");
        list1.insert(o2);
        addToMvccMap(o2);
        MVCCObject o3 = new MVCCObject(7, "version-7");
        list1.insert(o3);
        addToMvccMap(o3);
        MVCCObject o6 = new MVCCObject(9, "version-9");
        list1.insert(o6);
        addToMvccMap(o6);

        list1.commitObject(1, 6);
        MvccMap.get(1L).end = 6;
        list1.commitObject(3, 4);
        MvccMap.get(3L).end = 4;
        list1.commitObject(5, 6);
        MvccMap.get(5L).end = 6;
        list1.commitObject(7, 8);
        MvccMap.get(7L).end = 8;
        list1.commitObject(9, 12);
        MvccMap.get(9L).end = 12;

        //System.out.println("按开始版本号排序：");
        //list1.printList();
        //System.out.println("按结束版本号排序：");
        //list1.printCommitList();

        map.put("name", list1);
        return map;
    }

	public static void main (String args[]) throws IOException{
    	regionMap.put(1, DTGConstants.SERVER1 + ":" + DTGConstants.SERVER1PORT);
		regionMap.put(2, DTGConstants.SERVER2 + ":" + DTGConstants.SERVER2PORT);
		regionMap.put(3, DTGConstants.SERVER3 + ":" + DTGConstants.SERVER3PORT);
		new display();
	}

	private void addToMvccMap(MVCCObject o){
        Node n = new Node();
        n.start = o.getVersion();
        n.value = o.getValue();
        MvccMap.put(o.getVersion(), n);
    }

    private void addFrameShowrES(List<Integer> list){
		JFrame frame = new JFrame("道路信息管理系统");
		frame.setLayout(null);
		frame.setSize(650, 350);
		frame.setLocation(300, 250);
		frame.setVisible(true);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		background = new ImageIcon("D:\\picture\\7.jpg");// 背景图片
		JLabel label = new JLabel(background);// 把背景图片显示在一个标签里面
		// 把标签的大小位置设置为图片刚好填充整个面板
		label.setBounds(0, 0, background.getIconWidth(),
				background.getIconHeight());
		// 把内容窗格转化为JPanel，否则不能用方法setOpaque()来使内容窗格透明
		imagePanel = (JPanel) frame.getContentPane();
		imagePanel.setOpaque(false);
		frame.getLayeredPane().setLayout(null);
		// 把背景图片添加到分层窗格的最底层作为背景
		frame.getLayeredPane().add(label, new Integer(Integer.MIN_VALUE));
		frame.setResizable(false);

		JLabel memLable = new JLabel("运行详情", JLabel.CENTER);
		memLable.setBounds(0, 30, 650, 40);
		memLable.setFont(f3);
		frame.add(memLable);

		int i = 1;
		for(int t : list){
			JLabel readDataLable1 = new JLabel("存储节点：" + regionMap.get(i) + "     共完成  " + t + "  个操作");
			readDataLable1.setBounds(100, 50*i + 50, 550, 32);
			readDataLable1.setFont(f2);
			frame.add(readDataLable1);
			i++;
		}

	}

	private void getStoreInfo(){
		JFrame frame = new JFrame("道路交通数据演示");
		frame.setLayout(null);
		frame.setSize(650, 600);
		frame.setLocation(300, 250);
		frame.setVisible(true);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		JLabel memLable = new JLabel("集群状态", JLabel.CENTER);
		memLable.setBounds(0, 30, 650, 40);
		memLable.setFont(f3);
		frame.add(memLable);

		background = new ImageIcon("D:\\picture\\7.jpg");// 背景图片
		JLabel label = new JLabel(background);// 把背景图片显示在一个标签里面
		// 把标签的大小位置设置为图片刚好填充整个面板
		label.setBounds(0, 0, background.getIconWidth(),
				background.getIconHeight());
		// 把内容窗格转化为JPanel，否则不能用方法setOpaque()来使内容窗格透明
		imagePanel = (JPanel) frame.getContentPane();
		imagePanel.setOpaque(false);
		frame.getLayeredPane().setLayout(null);
		// 把背景图片添加到分层窗格的最底层作为背景
		frame.getLayeredPane().add(label, new Integer(Integer.MIN_VALUE));
		frame.setResizable(false);

		simpleTest test = new simpleTest();
		Map<String, List<Integer>> info = test.getStoreInfo();

		JLabel readDataLable = new JLabel("存储节点状态：");
		readDataLable.setBounds(70, 100, 500, 32);
		readDataLable.setFont(f2);
		frame.add(readDataLable);

		int i = 1;
		List<String> lost = new LinkedList<>();
		lost.add(DTGConstants.SERVER1 + ":" + DTGConstants.SERVER1PORT);
		lost.add(DTGConstants.SERVER2 + ":" + DTGConstants.SERVER2PORT);
		lost.add(DTGConstants.SERVER3 + ":" + DTGConstants.SERVER3PORT);
		for(Map.Entry<String, List<Integer>> t : info.entrySet()){
			if(lost.contains(t.getKey())){
				lost.remove(t.getKey());
			}
			JLabel readDataLable1 = new JLabel(t.getKey() + "  正运行以下Region:");
			readDataLable1.setBounds(100, 40*i + 95, 400, 32);
			readDataLable1.setFont(f2);
			frame.add(readDataLable1);
			String s = "";
			System.out.println(t.getValue().size());
			for(int m : t.getValue()){
				s = s + m + "   ";
			}
			JLabel readDataLable2 = new JLabel(s);
			readDataLable2.setBounds(470, 40*i + 95, 500, 32);
			readDataLable2.setFont(f2);
			frame.add(readDataLable2);
			i++;
		}

		JLabel readDataLable2 = new JLabel("中心节点Master地址 :   " + DTGConstants.MASTER);
		readDataLable2.setBounds(70, 40*i + 110, 500, 32);
		readDataLable2.setFont(f2);
		frame.add(readDataLable2);
		i++;

		JLabel readDataLable3 = new JLabel("备注：");
		readDataLable3.setBounds(70, 40*i + 130, 500, 32);
		readDataLable3.setFont(f2);
		frame.add(readDataLable3);
		i++;

		String beizhu = "系统一切正常！";
		if(lost.size() != 0){
			beizhu = "丢失了一些服务器 ： ";
		}
		JLabel readDataLable4 = new JLabel(beizhu);
		readDataLable4.setBounds(100, 40*i + 130, 500, 32);
		readDataLable4.setFont(f2);
		frame.add(readDataLable4);
		i++;
		if(lost.size() != 0){
			for(String s : lost){
				JLabel readDataLable5 = new JLabel(s);
				readDataLable5.setBounds(200, 40*i + 130, 500, 32);
				readDataLable5.setFont(f2);
				frame.add(readDataLable5);
				i++;
			}
		}


	}

	private void importTopoAction() throws TransactionException {
		simpleTest test = new simpleTest();
		Map<Integer, Object> res = test.importTopo(1);
		if(res == null){
			throw new TransactionException();
		}
		List<Integer> count = new ArrayList<>();
		for(int i = -2; i >= -4; i--){
			if(res.containsKey(i)){
				count.add((int)res.get(i));
			}
		}
		addFrameShowrES(count);
	}

	private void importTempDataAction(){
		simpleTest test = new simpleTest();
		Map<Integer, Object> res = test.importSomeData(1);
		List<Integer> count = new ArrayList<>();
		for(int i = -2; i >= -4; i--){
			if(res.containsKey(i)){
				count.add((int)res.get(i)*test.addTimes);
			}
		}
		addFrameShowrES(count);
	}

	private void readDataButtonAction(){
		simpleTest test = new simpleTest();
		String str = TimeField.getText();
		if(str.equals(""))
		{
			Object[] options = { "OK ", "CANCEL " };
			JOptionPane.showOptionDialog(null, "您还没有输入时间 ", "提示", JOptionPane.DEFAULT_OPTION,
					JOptionPane.WARNING_MESSAGE,null, options, options[0]);
		}
		int time = Integer.parseInt(str);
		if(time < 1 || time > 5*test.addTimes){
			Object[] options = { "OK ", "CANCEL " };
			JOptionPane.showOptionDialog(null, "请输入时间为1-" + 5*test.addTimes + "！", "提示", JOptionPane.DEFAULT_OPTION,
					JOptionPane.WARNING_MESSAGE,null, options, options[0]);
			return;
		}
		Map<Integer, Object> res =  test.readSomeData(time);
		Object[] options = { "OK ", "CANCEL " };
		JOptionPane.showOptionDialog(null, "获取到" + (res.size()-3) + "条数据！", "提示", JOptionPane.DEFAULT_OPTION,
				JOptionPane.WARNING_MESSAGE,null, options, options[0]);
		jta.setText("");
		for(int i = 0; i < 5028; i++){
			jta.append(res.get(i) + "\r\n");
		}
		List<Integer> count = new ArrayList<>();
		for(int i = -2; i >= -4; i--){
			if(res.containsKey(i)){
				count.add((int)res.get(i));
			}
		}
		addFrameShowrES(count);
	}

	private void readDataBetweenAction(){
		simpleTest test = new simpleTest();
		String str = textMaxField.getText();//获取输入内容
		String str2 = textMinField.getText();//获取输入内容
		String str3 = textMinTimeField.getText();
		//判断是否输入了
		if(str.equals(""))
		{
			Object[] options = { "OK ", "CANCEL " };
			JOptionPane.showOptionDialog(null, "您还没有输入最大值 ", "提示", JOptionPane.DEFAULT_OPTION,
					JOptionPane.WARNING_MESSAGE,null, options, options[0]);
			return;
		}else if(str2.equals("")){
			Object[] options = { "OK ", "CANCEL " };
			JOptionPane.showOptionDialog(null, "您还没有输入最小值 ", "提示", JOptionPane.DEFAULT_OPTION,
					JOptionPane.WARNING_MESSAGE,null, options, options[0]);
			return;
		}else if(str3.equals("")){
			Object[] options = { "OK ", "CANCEL " };
			JOptionPane.showOptionDialog(null, "您还没有输入时间 ", "提示", JOptionPane.DEFAULT_OPTION,
					JOptionPane.WARNING_MESSAGE,null, options, options[0]);
			return;
		}
		else{

			int time = Integer.parseInt(str3);
			if(time < 1 || time > 5*test.addTimes){
				Object[] options = { "OK ", "CANCEL " };
				JOptionPane.showOptionDialog(null, "请输入时间为1-" + 5*test.addTimes + "！", "提示", JOptionPane.DEFAULT_OPTION,
						JOptionPane.WARNING_MESSAGE,null, options, options[0]);
				return;
			}

			Map<Integer, Object> res =  test.readSomeLimitData(Integer.parseInt(str), Integer.parseInt(str2), time);
			int ressize = res.size() - 3;
			Object[] options = { "OK ", "CANCEL " };
			JOptionPane.showOptionDialog(null, "获取到" + ressize + "条数据！", "提示", JOptionPane.DEFAULT_OPTION,
					JOptionPane.WARNING_MESSAGE,null, options, options[0]);
			jta.setText("");
			for(int i = 0; i < ressize; i++){
				Object s = res.get(i);
				jta.append(s + "\r\n");
			}
			List<Integer> count = new ArrayList<>();
			for(int i = -2; i >= -4; i--){
				if(res.containsKey(i)){
					count.add((int)res.get(i));
				}
			}
			addFrameShowrES(count);
		}
	}

	private void simpleChangeAction(int id, String property, int start, int end, String value) throws TransactionException {
		DTGDatabase db = new DTGDatabase();
		db.init("127.0.0.1", 10086, "D:\\garbage");

		try (DTGTransaction tx = db.CreateTransaction()){

			RelationshipAgent r = db.getRelationshipById(id);
			r.setTemporalProperty(property, start,end, value);
			Map res = tx.start();
			if(res == null){
				throw new TransactionException();
			}
			List<Integer> count = new ArrayList<>();
			for(int i = -2; i >= -4; i--){
				if(res.containsKey(i)){
					count.add((int)res.get(i));
				}
			}
			addFrameShowrES(count);
		}

	}

	private Object simpleReadAction(int id, String property, int time) throws TransactionException {
		DTGDatabase db = new DTGDatabase();
		db.init("127.0.0.1", 10086, "D:\\garbage");

		try (DTGTransaction tx = db.CreateTransaction()){

			RelationshipAgent r = db.getRelationshipById(id);
			int t = r.getTemporalProperty(property, time);
			Map res = tx.start();
			if(res == null){
				throw new TransactionException();
			}
			List<Integer> count = new ArrayList<>();
			for(int i = -2; i >= -4; i--){
				if(res.containsKey(i)){
					count.add((int)res.get(i));
				}
			}
			addFrameShowrES(count);
			return res.get(t);
		}
	}

	class Node{
	    long start;
	    long end;
	    Object value;
    }

}
