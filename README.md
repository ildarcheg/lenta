# Анализируй это. Lenta.ru
### What, How, Why

На мой взгляд, применяя вопросы "What, How and Why" в отношении какой-либо задачи, самое важное будет "Why". Именно ответ на этот вопрос определит, чего вы в итоге добьетесь и как вы это сделаете. В моем случае, коротким ответом на вопрос "почему" будет "получение опыта". Более развернутым же объяснением будет "выполнение какого-либо реального задания, в рамках которого я смогу применить навыки, полученные во время обучения, а так же получить результат, который я бы смог показывать в качестве подтверждения своих умений". Хотя на самом деле вся интрига "а зачем тебе это все" раскрыта  [вот здесь (оффтоп)](https://habrahabr.ru/post/329906). Там же будет и мой бэкграунд.

Конечно, определившись внутри себя, что мне нужны практика и портфолио, стоило бы схватить пару датасетов, коих сейчас море, и анализировать, анализировать, анализировать... Но прикинув в голове свои скилы по непосредственному анализу и вспомнив, что анализ это только 20-30% времени и остальное это поиск-сбор-очиска-подготовка данных, решил взяться за второе. Да и хотелось что-то особенное, возможно даже интересное, в отличии от анализа проданных в США авиабилетов за последние 30 лет или статистику арестов.

В качестве объекта исследования была выбрана [Lenta.ru](https://lenta.ru). От части, потому что я являюсь ее давним читателем, хоть и регулярно плююсь от того шлака, который проскакивает мимо редакторов (если таковые там вообще имеются). От части, потому что она показалась относительно легким для data mining. Однако если быть честным, то подходя к выбору объекта я практически не учитывал вопросы "а что я буду с этой датой делать" и "какие вопросы буду задавать". И связано это с тем, что на текущий момент я более-менее освоил только Getting and Cleaning Data и мои знания в части анализа очень скудны. Я конечно представлял себе, что как минимум могу ответить на вопрос "как изменилась среднедневное количество публикуемых новостей за последние 5-10 лет", но дальше этого я не задумывался.  

И так, эта статья будет посвящена добыванию и очистке данных, которые будут пригодны для анализа. 

### Grabbing
Первым делом мне необходимо было определиться, как сграббить и распарсить содержимое страниц ресурса. Google подсказал, что оптимальным для этого будет использование пакета [rvest](https://cran.r-project.org/web/packages/rvest/rvest.pdf), который одновременно позволяет получить текст страницы по ее адресу и при помощи xPath выдернуть содержимое нужных мне полей. Конечно, продвинувшись дальше, мне пришлось разбить эту задачу на две - получение страниц и непосредственный парсинг, но это я понял позже, а пока первым шагом было получение списка ссылок на сами статьи.

После недолгого изучения, на сайте был обнаружен раздел "Архив", который при помощи простого скрипта переадресовывал меня на страницу, содержащую ссылки все новости за определенную дату и путь к этой странице выглядел как https://lenta.ru/2017/07/01/ или https://lenta.ru/2017/03/09/. Оставалось только пройтись по всем этим страницам и получить эти самые новостные ссылки. В чем мне и помог этот нехитрый код:
```R
  # prepare vector of links of archive pages in https://lenta.ru//yyyy/mm/dd/ format
  dayArray <- seq(as.Date(articlesStartDate), as.Date(articlesEndDate), 
                  by="days")
  archivePagesLinks <- paste0(baseURL, "/", year(dayArray), 
                      "/", formatC(month(dayArray), width = 2, format = "d", flag = "0"), 
                      "/", formatC(day(dayArray), width = 2, format = "d", flag = "0"), 
                      "/")
  articlesLinks <- c()
  for (i in 1:length(archivePagesLinks)) {
    pg <- read_html(archivePagesLinks[i], encoding = "UTF-8")
    total <- html_nodes(pg, 
              xpath=".//section[@class='b-longgrid-column']//div[@class='titles']//a") %>% 
              html_attr("href")   
    articlesLinks <- c(articlesLinks, total)
    saveRDS(articlesLinks, file.path(tempDataFolder, "tempArticlesLinks.rds"))
  }
  articlesLinks <- paste0(baseURL, articlesLinks)
  writeLines(articlesLinks, file.path(tempDataFolder, "articles.urls"))
```
Сгенерировав массив дат с `2010-01-01` по `2017-06-30` и преобразовав `archivePagesLinks`, я получил ссылки на все так называемые "архивные страницы":

```
> head(archivePagesLinks)
[1] "https://lenta.ru/2010/01/01/"
[2] "https://lenta.ru/2010/01/02/"
[3] "https://lenta.ru/2010/01/03/"
[4] "https://lenta.ru/2010/01/04/"
[5] "https://lenta.ru/2010/01/05/"
[6] "https://lenta.ru/2010/01/06/"
> length(archivePagesLinks)
[1] 2738
```
При помощи метода `read_html` я в цикле "скачал" содержимое страниц в буфер, а при помощи методов `html_nodes` и `html_attr` получил непосредственно ссылки на статьи:
```
> head(articlesLinks)
[1] "https://lenta.ru/news/2009/12/31/kids/"     
[2] "https://lenta.ru/news/2009/12/31/silvio/"   
[3] "https://lenta.ru/news/2009/12/31/postpone/" 
[4] "https://lenta.ru/photo/2009/12/31/meeting/" 
[5] "https://lenta.ru/news/2009/12/31/boeviks/"  
[6] "https://lenta.ru/news/2010/01/01/celebrate/"
> length(articlesLinks)
[1] 379862
```

После получения первых результатов я осознал проблему. Код, приведенный выше, выполнялся примерно `40 мин`. С учетом размера массива в `2738` ссылок, который был обработан за это время, можно посчитать, что для обработки `379862` ссылок уйдет `5550` минут или `92 с половиной часа`, что согласитесь, ни в какие ворота... Встроенные методы `readLines {base}` и `download.file {utils}`, которые позволяли просто получить текст, давали схожие результаты. Метод `htmlParse {XML}`, который позволял аналогично `read_html` продолжить парсинг содержимого, также ситуацию не улучшил. Тот же результат с использованием `getURL {RCurl}`. Тупик.

В поисках решения проблемы, я и гугл решили посмотреть в сторону "параллельного" исполнения моих запросов, так в момент работы кода ни сеть, ни память, ни процессор не были загружены. Гугл подсказал копнуть в сторону `parallel-package {parallel}`. Несколько часов изучения и тестов показали, что профита с запуском даже в двух "параллельных" потоках почему нет. Обрывочные сведения в гугле рассказали, что с помощью этого пакета можно распараллелить какие-нибудь вычисления или манипуляции с данными, но при работе с диском или внешним источником все запросы выполняются в рамках одного процесса и выстраиваются в очередь (могу ошибаться в понимании ситуации). Да и как понял, даже если бы тема и взлетела, ожидать увеличение производительности стоило только кратно имеющимся ядрам, т.е. даже при наличии 8 штук (которых у меня и не было) и реальной параллельности, курить мне предстояло примерно 690 минут. 

Следующей идеей было запустить параллельно несколько процессов R, в которых бы обрабатывалась своя часть большого списка ссылок. Однако гугл на вопрос "как из сессии R запустить несколько новых сессий R" ничего не сказал. Подумал еще над вариантом запуска R-скрипта через командную строку, но опыт работы с CMD были на уровне "набери dir и получишь список файлов в папке". Я снова оказался в тупике. 

Когда гугл перестал мне выдавать новые результаты, я с чистой совестью решил обратиться за помощью к залу. Так как гугл довольно часто выдавал [stackoverflow](https://stackoverflow.com), я решил попытать счастье именно там. Имея опыт общения на тематических форумах и зная реакцию на вопросы новичков, попытался максимально четко и ясно изложить [проблему](https://stackoverflow.com/questions/39180106/i-have-to-grab-plantext-from-over-290k-webpages-is-there-a-way-to-improve-the-s). И о чудо, спустя какие несколько часов я получил от [Bob Rudis](https://rud.is/) более чем развернутый ответ, который после подстановки в мой код, практически полностью решал мою задачу. Правда с оговоркой: я совершенно не понимал как он работает. Я первый раз слышал про `wget`, не понимал, что в коде делают с `WARC` и зачем в метод передают функцию (повторюсь, семинариев не кончал и в моем предыдущем языке таки финты не использовались). Однако если долго-долго смотреть на код, то просветление все таки приходит. А добавив попытки выполнять его по кускам, разбирая функцию за функцией, можно добиться определенных результатов. Ну а с `wget` мне помог справиться все тот же гугл. 

В итоге суть решения свелась к следующему - предварительно подготовленный файл, содержащий ссылки на статьи, подсовывался команде `wget`:
```
  wget --warc-file=lenta -i lenta.urls
```
Непосредственно код выполнения выглядел так:
```R
  system("wget --warc-file=lenta -i lenta.urls", intern = FALSE)
```
После выполнения, я получал кучу файлов (по одному на каждую переданную ссылку) с html содержимым веб-страниц. Также в моем распоряжении был запакованный `WARC`, который содержал в себе лог общения с ресурсом, а так же то самое содержимое веб-страниц. Именно `WARC` и предлагал парсить [Bob Rudis](https://rud.is/). То что нужно. Причем у меня оставилсь копии прочитанных страниц, что делало возможно их повторное чтение.

Первые замеры производительности показали, что для закачки `2000` ссылок было потрачено `10 минут`, методом экстраполяции (давно хотел использовать это слово) получал `1890 минут` для всех статей - "олмост три таймс фастер бат нот энаф" - почти в три раза быстрее, но все равно недостаточно. Вернувшись на пару шагов назад и протестив новый механизм с учетом `parallel-package {parallel}`, понял, что и здесь профита не светит.

Оставался ход конем. Запуск нескольких по-настоящему параллельных процессов. С учетом того, что от "reproducible research" (принцип, о котором говорили на курсах) шаг в сторону я уже сделал (использовав внешнюю программу wget и фактически привязав выполнение к среде Windows), я решил сделать еще один шаг и снова вернуться к идее запуска параллельных процессов, однако уже вне R. Как заставить CMD файл выполнять несколько последовательных команд, не дожидаясь выполнения предыдущей (читай параллельно), спустя пару часов гуглинга рассказал все тот же [stackoverflow](https://stackoverflow.com). Оказалось, что замечательная команда `START` позволяет запустить команду на выполнение в отдельном окне. Вооружившись этим знанием, разродился следующим кодом:

```R
  articlesLinks <- readLines(file.path(tempDataFolder, "articles.urls"))
  dir.create(warcFolder, showWarnings = FALSE)
  
  # split up articles links array by 10K links 
  numberOfLinks <- length(articlesLinks)
  digitNumber <- nchar(numberOfLinks)
  groupSize <- 10000
  filesGroup <- seq(from = 1, to = numberOfLinks, by = groupSize)
  cmdCodeAll <- c()
  
  for (i in 1:length(filesGroup)) {
    
    # prepare folder name as 00001-10000, 10001-20000 etc
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfLinks)
    leftPartFolderName <- formatC(firstFileInGroup, width = digitNumber, 
                                  format = "d", flag = "0")
    rigthPartFolderName <- formatC(lastFileInGroup, width = digitNumber, 
                                   format = "d", flag = "0")
    subFolderName <- paste0(leftPartFolderName, "-", rigthPartFolderName)
    subFolderPath <- file.path(downloadedArticlesFolder, subFolderName)
    dir.create(subFolderPath)

    # write articles.urls for each 10K folders that contains 10K articles urls
    writeLines(articlesLinks[firstFileInGroup:lastFileInGroup], 
               file.path(subFolderPath, "articles.urls"))
    
    # add command line in CMD file as:
    # START wget --warc-file=warc\000001-010000 -i 000001-010000\list.urls -P 000001-010000
    cmdCode <-paste0("START ..\\wget --warc-file=warc\\", subFolderName," -i ", 
                     subFolderName, "\\", "articles.urls -P ", subFolderName)
    cmdCodeAll <- c(cmdCodeAll, cmdCode)
  }
  
  cmdFile <- file.path(downloadedArticlesFolder, "start.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start downloading."))
  print("wget.exe should be placed in working directory.")
```
Этот код разбивает массив ссылок на блоки по 10000 штук (в моем случае получилось 38 блоков). Для каждого блока в цикле создается папка вида  `00001-10000`, `10001-20000` и т.д, в которую складывается свой собственный файл "articles.urls" (со своим 10-тысячным набором ссылок) и туда же попадут скачанные файлы. В этом же цикле собирается CMD файл, который должен одновременно запустить 38 окон:
```
START ..\wget --warc-file=warc\000001-010000 -i 000001-010000\articles.urls -P 000001-010000
START ..\wget --warc-file=warc\010001-020000 -i 010001-020000\articles.urls -P 010001-020000
START ..\wget --warc-file=warc\020001-030000 -i 020001-030000\articles.urls -P 020001-030000
...
START ..\wget --warc-file=warc\350001-360000 -i 350001-360000\articles.urls -P 350001-360000
START ..\wget --warc-file=warc\360001-370000 -i 360001-370000\articles.urls -P 360001-370000
START ..\wget --warc-file=warc\370001-379862 -i 370001-379862\articles.urls -P 370001-379862
```
Запуск запуск сформированного CMD файла запускает ожидаемые 38 окон с командой `wget` и дает следующую загрузку компьютера со следующими характеристиками `3.5GHz Xeon E-1240 v5, 32Gb, SSD, Windows Server 2012`

![Загрузка компьютера в момент одновременной закачки страниц](images/download_performance.jpg)

Итоговое время `180 минут` или `3 часа`. "Костыльный" параллелизм дал почти `10-кратный` выигрыш по сравнению с однопоточным выполнением `wget` и `30-кратный` выигрыш относительно изначального варианта использования `read_html {rvest}`. Это была первая маленькая победа и подобный "костыльный" подход мне пришлось применить потом еще один раз.

Результатом выполнения на жестком диске был представлен следующим:
```
> indexFiles <- list.files(downloadedArticlesFolder, full.names = TRUE, recursive = TRUE, pattern = "index")
> length(indexFiles)
[1] 379703
> sum(file.size(indexFiles))/1024/1024
[1] 66713.61
> warcFiles <- list.files(downloadedArticlesFolder, full.names = TRUE, recursive = TRUE, pattern = "warc")
> length(warcFiles)
[1] 38
> sum(file.size(warcFiles))/1024/1024
[1] 18770.4
```
Что означает `379703` скачанных веб-страниц общим размером `66713.61MB` и `38` сжатых `WARC`-файлы общим размером `18770.40MB`. Нехитрое вычисление показало, что я "потерял" `159` страниц. Возможно их судьбу можно узнать распарсив `WARC`-файлы по примеру от [Bob Rudis](https://rud.is/), но я решил списать их на погрешность и пойти своим путем, распарсивая непосредственно `379703` файлов.

### Parsing

Прежде чем что-то выдергивать со скачанной страницы, мне предстояло определить что именно выдергивать, какая именно информация мне может быть интересна. После долго изучения содержимого страниц, подготовил следующий код:
```R
ReadFile <- function(filename) {
  pg <- read_html(filename, encoding = "UTF-8")
  
  metaTitle <- html_nodes(pg, xpath=".//meta[@property='og:title']") %>%
    html_attr("content") %>% 
    SetNAIfZeroLength() 
  metaType <- html_nodes(pg, xpath=".//meta[@property='og:type']") %>% 
    html_attr("content") %>% 
    SetNAIfZeroLength()
  metaDescription <- html_nodes(pg, xpath=".//meta[@property='og:description']") %>% 
    html_attr("content") %>% 
    SetNAIfZeroLength()
  rubric <- html_nodes(pg, xpath=".//div[@class='b-subheader__title js-nav-opener']") %>% 
    html_text() %>% 
    SetNAIfZeroLength()
  
  scriptContent <- html_nodes(pg, xpath=".//script[contains(text(),'chapters: [')]") %>% 
    html_text() %>% 
    strsplit("\n") %>% 
    unlist()
  
  if (is.null(scriptContent[1])) {
    chapters <- NA
  } else if (is.na(scriptContent[1])) {
    chapters <- NA
  } else {
    chapters <- scriptContent[grep("chapters: ", scriptContent)] %>% unique()
  }
  
  title <- html_nodes(pg, xpath=".//head/title") %>% 
    html_text() %>% 
    SetNAIfZeroLength()
  
  #shareFB <- html_nodes(pg, xpath=".//div[@data-target='fb']")
  #shareVK <- html_nodes(pg, xpath=".//div[@data-target='vk']")
  #shareOK <- html_nodes(pg, xpath=".//div[@data-target='ok']")
  #shareTW <- html_nodes(pg, xpath=".//div[@data-target='tw']")
  #shareLJ <- html_nodes(pg, xpath=".//div[@data-target='LJ']")

  articleBodyNode <- html_nodes(pg, xpath=".//div[@itemprop='articleBody']")
  
  plaintext <- html_nodes(articleBodyNode, xpath=".//p") %>% 
    html_text() %>% 
    paste0(collapse="") 
  if (plaintext == "") {
    plaintext <- NA
  }
  
  plaintextLinks <- html_nodes(articleBodyNode, xpath=".//a") %>% 
    html_attr("href") %>% 
    unique() %>% 
    paste0(collapse=" ")
  if (plaintextLinks == "") {
    plaintextLinks <- NA
  }
  
  additionalLinks <- html_nodes(pg, xpath=".//section/div[@class='item']/div/..//a") %>% 
    html_attr("href") %>% 
    unique() %>% 
    paste0(collapse=" ")
  if (additionalLinks == "") {
    additionalLinks <- NA
  }
  
  imageNodes <- html_nodes(pg, xpath=".//div[@class='b-topic__title-image']")
  imageDescription <- html_nodes(imageNodes, xpath="div//div[@class='b-label__caption']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength()
  imageCredits <- html_nodes(imageNodes, xpath="div//div[@class='b-label__credits']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength() 
  
  if (is.na(imageDescription)&is.na(imageCredits)) {
    videoNodes <- html_nodes(pg, xpath=".//div[@class='b-video-box__info']")
    videoDescription <- html_nodes(videoNodes, xpath="div[@class='b-video-box__caption']") %>% 
      html_text() %>% 
      unique() %>% 
      SetNAIfZeroLength()
    videoCredits <- html_nodes(videoNodes, xpath="div[@class='b-video-box__credits']") %>% 
      html_text() %>% 
      unique() %>% 
      SetNAIfZeroLength() 
  } else {
    videoDescription <- NA
    videoCredits <- NA
  }
  
  url <- html_nodes(pg, xpath=".//head/link[@rel='canonical']") %>% 
    html_attr("href") %>% 
    SetNAIfZeroLength()
  
  authorSection <- html_nodes(pg, xpath=".//p[@class='b-topic__content__author']")
  authors <- html_nodes(authorSection, xpath="//span[@class='name']") %>% 
    html_text() %>% 
    SetNAIfZeroLength()
  if (length(authors) > 1) {
    authors <- paste0(authors, collapse = "|")
  }
  authorLinks <- html_nodes(authorSection, xpath="a") %>% html_attr("href") %>% SetNAIfZeroLength()
  if (length(authorLinks) > 1) {
    authorLinks <- paste0(authorLinks, collapse = "|")
  }
  
  datetimeString <- html_nodes(pg, xpath=".//div[@class='b-topic__info']/time[@class='g-date']") %>% 
    html_text() %>% 
    unique() %>% 
    SetNAIfZeroLength()
  datetime <- html_nodes(pg, xpath=".//div[@class='b-topic__info']/time[@class='g-date']") %>% 
    html_attr("datetime") %>% unique() %>% SetNAIfZeroLength()
  if (is.na(datetimeString)) {
    datetimeString <- html_nodes(pg, xpath=".//div[@class='b-topic__date']") %>% 
      html_text() %>% 
      unique() %>% 
      SetNAIfZeroLength()
  }

  data.frame(url = url,
             filename = filename, 
             metaTitle= metaTitle,
             metaType= metaType,
             metaDescription= metaDescription,
             rubric= rubric,
             chapters = chapters,
             datetime = datetime,
             datetimeString = datetimeString,
             title = title, 
             plaintext = plaintext, 
             authors = authors, 
             authorLinks = authorLinks,
             plaintextLinks = plaintextLinks,
             additionalLinks = additionalLinks, 
             imageDescription = imageDescription,
             imageCredits = imageCredits,
             videoDescription = videoDescription,
             videoCredits = videoCredits,
             stringsAsFactors=FALSE)
}
```
Для начала заголовок, который я получаю его из двух мест. Изначально я получал из `title` что-то вида `Швеция признана лучшей страной мира для иммигрантов: Общество: Мир: Lenta.ru`, в надежде разбить заголовок на непосредственный заголове и на рубрику с подрубрикой. Однако потом решил под подстраховки подтянуть заголовок в чистом виде из метаданных страницы.

Дату и время я получаю из `<time class="g-date" datetime="2017-07-10T12:35:00Z" itemprop="datePublished" pubdate=""> 15:35, 10 июля 2017</time>`, причем для подстраховки решил получить не только `2017-07-10T12:35:00Z`, но и текстовое представление `15:35, 10 июля 2017` и как оказалось не зря. Это текстовое представление позволило получать время статьи для случаев, когда `time[@class='g-date']` по какой-то причине на странице отсутствовал.

Авторство в статьях отмечается крайне редко, однако я все равно решил выдернуть эту информацию, на всякий случай. Также мне показалось интересным ссылки, которые появлялись в текстах самих статей и под ними в разделе "Ссылки по теме". На правильный парсинг информации о картинках и видео в начале статьи потратил чуть больше времени, чем хотелось бы, но тоже на всякий случай. 

Для получения рубрики и подрубрики я решил подстраховаться и сохранить строку `chapters: ["Мир","Общество","lenta.ru:_Мир:_Общество:_Швеция_признана_лучшей_страной_мира_для_иммигрантов"], // Chapters страницы`, показалось, что и нее выдернуть "Мир" и "Общество" будет чуть легче, чем из заголовка.

Особый интерес у меня вызвало количество расшариваний, количество комментариев к статье и конечно сами комментарии (словарное содержимое, временная активность), так как именно это было единственной информацией о том, как читатели реагировали на статью. Но именно самое интересное у меня и не получилось. Счетчики количества шар и камменнтов устанавливаются скриптом, который выполняетя после загрузки страницы. А весь мой суперхитрый код выкачивал страницу до этого момента, оставляя соответствующие поля пустыми. Комметарии также подгружаются скриптом, кроме того они отключаются для статей спустя какое-то время и получить их не представляется возможным. Но я еще работаю на этим вопросом, так как все-таки хочется увидеть зависимость наличия слов Украина/Путин/Кандолиза и количества срача в камментах.

Вот в принципе и вся информация, которую я посчитал полезной.

Следущий код позволил мне запустить парсинг фалов, находящейся в первой парке (из 38):

```R
ReadFilesInFolder <- function(folderNumber) {
  folders <- list.files(downloadedArticlesFolder, full.names = FALSE, 
                        recursive = FALSE, pattern = "-")
  folderName <- folders[folderNumber]
  currentFolder <- file.path(downloadedArticlesFolder, folderName)
  files <- list.files(currentFolder, full.names = TRUE, 
                      recursive = FALSE, pattern = "index")
  
  numberOfFiles <- length(files)
  print(numberOfFiles)
  groupSize <- 1000
  filesGroup <- seq(from = 1, to = numberOfFiles, by = groupSize)
  dfList <- list()
  for (i in 1:length(filesGroup)) {
    firstFileInGroup <- filesGroup[i]
    lastFileInGroup <- min(firstFileInGroup + groupSize - 1, numberOfFiles)
    print(paste0(firstFileInGroup, "-", lastFileInGroup))
    dfList[[i]] <- map_df(files[firstFileInGroup:lastFileInGroup], ReadFile)
  }
  df <- bind_rows(dfList)
  write.csv(df, file.path(parsedArticlesFolder, paste0(folderName, ".csv")), 
            fileEncoding = "UTF-8")
}
```
Получив адрес папки вида `00001-10000` и ее содержимое, я разбивал массив файлов на блоки по `1000` и в цикле при помощи `map_df` запускал свою функцию `ReadFile` для каждого такого блока.

Замеры показали, что для обработки `10000` статей, требуется примерно `8 минут` (причем `95%` времени занимал метод `read_html`). Все той же экстраполяцией получил `300 минут` или `5 часов`.

И вот обещанный второй ход конем. Запуск понастоящему параллельных сессий R (благо опыт общения с CMD уже имелся). Поэтому при помощи этого скрипта, я получил необходимый CMD файл:
```R
  # get list of folders that contain downloaded articles
  folders <- list.files(downloadedArticlesFolder, full.names = FALSE, 
                        recursive = FALSE, pattern = "-")
  
  # create CMD contains commands to run parse.R script with specified folder number
  nn <- 1:length(folders)
  cmdCodeAll <- paste0("start C:/R/R-3.4.0/bin/Rscript.exe ", 
                       file.path(getwd(), "parse.R "), nn)
  
  cmdFile <- file.path(downloadedArticlesFolder, "parsing.cmd")
  writeLines(cmdCodeAll, cmdFile)
  print(paste0("Run ", cmdFile, " to start parsing."))
```

Вида:
```
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 1
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 2
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 3
...
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 36
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 37
start C:/R/R-3.4.0/bin/Rscript.exe C:/Users/ildar/lenta/parse.R 38
```

Который в свою очередь запускал на выполнение 38 скриптов:
```R
args <- commandArgs(TRUE)
n <- as.integer(args[1])

# Set workling directory and locale for macOS and Windows
if (Sys.info()['sysname'] == "Windows") {
  workingDirectory <- paste0(Sys.getenv("HOMEPATH"), "\\lenta") 
  Sys.setlocale("LC_ALL", "Russian")
} else {
  workingDirectory <- ("~/lenta")
  Sys.setlocale("LC_ALL", "ru_RU.UTF-8")
}
setwd(workingDirectory)

source("get_lenta_articles_list.R")

ReadFilesInFolder(n)
```

![Загрузка компьютера в момент одновременного парсинга страниц](images/parse_performance.jpg)

Подобное распараллеливание, позволило `100%` загрузить сервер и выполнить поставленную задачу за `30 мин`. Очередной `10-кратный` выигрыш.

Остается только выполнить скрипт, который объединит 38 только что созданных файлов:
```R
  files <- list.files(parsedArticlesFolder, full.names = TRUE, recursive = FALSE)
  dfList <- c()
  for (i in 1:length(files)) {
    file <- files[i]
    print(file)
    dfList[[i]] <- read.csv(file, stringsAsFactors = FALSE, encoding = "UTF-8")
  }
  df <- bind_rows(dfList)
  write.csv(df, file.path(parsedArticlesFolder, "untidy_articles_data.csv"), 
            fileEncoding = "UTF-8")
```

И в итоге у нас на диске `1.739MB` неочищеной и неприведенной даты:
```
> file.size(file.path(parsedArticlesFolder, "untidy_articles_data.csv"))/1024/1024
[1] 1739.047
```

Что же внутри?
```
> str(dfM, vec.len = 1)
'data.frame':   379746 obs. of  21 variables:
 $ X.1             : int  1 2 ...
 $ X               : int  1 2 ...
 $ url             : chr  "https://lenta.ru/news/2009/12/31/kids/" ...
 $ filename        : chr  "C:/Users/ildar/lenta/downloaded_articles/000001-010000/index.html" ...
 $ metaTitle       : chr  "Новым детским омбудсменом стал телеведущий Павел Астахов" ...
 $ metaType        : chr  "article" ...
 $ metaDescription : chr  "Президент РФ Дмитрий Медведев назначил нового уполномоченного по правам ребенка в России. Вместо Алексея Голова"| __truncated__ ...
 $ rubric          : logi  NA ...
 $ chapters        : chr  "          chapters: [\"Россия\",\"Страница_подраздела\",\"lenta.ru:_Россия:_Новым_детским_омбудсменом_стал_теле"| __truncated__ ...
 $ datetime        : chr  "2009-12-31T21:24:33Z" ...
 $ datetimeString  : chr  " 00:24,  1 января 2010" ...
 $ title           : chr  "Новым детским омбудсменом стал телеведущий Павел Астахов: Россия: Lenta.ru" ...
 $ plaintext       : chr  "Президент РФ Дмитрий Медведев назначил нового уполномоченного по правам ребенка в России. Вместо Алексея Голова"| __truncated__ ...
 $ authors         : chr  NA ...
 $ authorLinks     : chr  NA ...
 $ plaintextLinks  : chr  "http://www.interfax.ru/" ...
 $ additionalLinks : chr  "https://lenta.ru/news/2009/12/29/golovan/ https://lenta.ru/news/2009/09/01/children/" ...
 $ imageDescription: chr  NA ...
 $ imageCredits    : chr  NA ...
 $ videoDescription: chr  NA ...
 $ videoCredits    : chr  NA ...
```

Парсинг завершен. Осталось привести эту дату к состоянию, пригодному для анализа.

### Cleaning

...