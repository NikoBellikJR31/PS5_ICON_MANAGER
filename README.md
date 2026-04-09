<div align="center">

<img src="assets/banner.png" width="500"/>

# 🎮 PS5 Icon Manager

Remplacez les icônes, backgrounds et visuels XMB de vos jeux PS5 — directement depuis votre navigateur.

<br>

<img src="https://img.shields.io/badge/PS5-Icon%20Manager-003087?style=for-the-badge&logo=playstation&logoColor=white" />
<img src="https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white" />
<img src="https://img.shields.io/badge/FTP-Enabled-success?style=for-the-badge" />
<img src="https://img.shields.io/badge/Windows-Compatible-0078D6?style=for-the-badge&logo=windows&logoColor=white" />

</div>

---

## 🎥 Demo

<div align="center">

<a href="https://youtu.be/ezQQhMrpd0Q">
  <img src="assets/demo.gif" width="600"/>
</a>

</div>

👉 Clique sur l’image pour voir la vidéo complète

---

## 🚀 Fonctionnalités

- 🎨 Modification des icônes (`icon0`)
- 🖼️ Modification des backgrounds (`pic1`)
- 🧩 Custom du background XMB

👉 Glissez n’importe quelle image, le tool gère tout automatiquement :
- Conversion
- Redimensionnement
- Renommage
- Placement
- Backup des images avant changement puis restauration backup

  Dans l'ideal pour le redimensionnement essayer de mettre au minimum des images proche des tailles respectif , upscale ces pas terrible.

  Le fonctionnement est simple , vous double cliquer sur le .bat , vous aller sur votre navigateur ( j'utilise Opera Gx pour l'historique des ip  ) vous taper localhost:8001 ou ctrl cliquer sur le link sur le cmd.

   Vous rentrer ip/port , sa va scanner vos applications , vous cliquer sur votre jeu ou applications, cela va trouver vos images , vous changer a votre souhaits les images qu'il a '''trouver d'origine''' vous cliquer sur '''remplacer''' pas besoin de renommer ou d'avoir specialement un png ou dds le tool se charge de tout convertir redimensionner ... et parallelement il creer des .bak de vos icons d'origine ,  si vous souhaiter les remettre a l'etat d'origine  ultérieurement vous cliquer sur 'restaurer backups' vous aurez le choix de restaurer que la jaquette ou seulement le background xmb celui du demarrage ... au choix

  Pour les jeux ou homebrews, prenons pour exemple Itemflow ou homebrew store il non pas de background xmb de base ces des generique , le tool ne vous trouvera rien non plus , vous pouvez rajouter une image tout de meme , vous cliquer sur remplacer vous mettez une image , ensuite vous cliquer sur forcer chemins et vous aurez votre image xmb ( peut ne pas fonctionner pour tous je n'ai pas tous regarder encore ) netflix youtube autoloader fonctionnel ...
  Certains changement demande de redemarrer la console
  Vous avez la touche supprimer pour effacer directement , quand vous supprimer sa enleve aussi dans la db pour ne pas avoir des entrees fantome 
Pour vos modifications d'image il y a gimp et paint.net
 
---

## ⚙️ Installation

### Prérequis

- Python ( j'ai mis le 3.14 )
- Windows 
- PS5 avec FTP activer

   Version beta 2.1 car j'ai differente versions avec d'autres options en test , possible bug 

versions exe avec modifications des noms des fonts et at9

git clone https://github.com/votre-pseudo/ps5-icon-manager.git
cd ps5-icon-manager
