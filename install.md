### LibreOffice 24.2.7.2 安裝
# Ubuntu apt 安裝：
sudo apt update
sudo apt install -y libreoffice
soffice --version
# 官方舊版封存檔安裝：
cd /tmp
wget https://downloadarchive.documentfoundation.org/libreoffice/old/24.2.7.2/deb/x86_64/LibreOffice_24.2.7.2_Linux_x86-64_deb.tar.gz
tar -xzf LibreOffice_24.2.7.2_Linux_x86-64_deb.tar.gz
cd LibreOffice_24.2.7.2_Linux_x86-64_deb/DEBS
sudo apt install -y ./*.deb
soffice --version


### pandoc 安裝
cd /tmp
wget https://github.com/jgm/pandoc/releases/download/3.9.0.2/pandoc-3.9.0.2-1-amd64.deb
sudo apt install -y ./pandoc-3.9.0.2-1-amd64.deb
pandoc --version | head -n 1


### sqlcmd 安裝
sudo apt update
sudo ACCEPT_EULA=Y apt install -y mssql-tools18 unixodbc-dev
# 安裝後再加入 PATH：
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc
# 最後確認：
which sqlcmd
sqlcmd -?

