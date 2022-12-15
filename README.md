# EMTF-DQM-Website
A web-tool for querying CERN's CMS Data Quality Monitoring System


![Screenshot from 2022-11-22 14-57-57](https://user-images.githubusercontent.com/26662618/203333553-7f1adc0b-4509-4a58-b2a6-89a8c4c72c57.png)

![Screenshot from 2022-11-22 14-58-21](https://user-images.githubusercontent.com/26662618/203333565-84cfafa7-aca2-4611-b380-d5f841c77626.png)

![Screenshot from 2022-11-22 14-58-33](https://user-images.githubusercontent.com/26662618/203333573-12777ebe-4482-47e6-ba1f-3d522ace7711.png)

# Host the website yourself (If and when csctiming.cern.ch goes down)

1. Register CERN Instance with hostname of choice: https://openstack.cern.ch/project/ . Use C8 (Centos8) OS Source. m2.small size is fine. Create a key pair to eventually use to ssh.

2. SSH to this instance using your created key password. Clone git repo.

3. Get Grid Certificate Key & certificate: https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookStartingGrid

4. Get CERN Root Certificate: https://cafiles.cern.ch/cafiles/

5. Place these three files in a directory (outside of the repo). In backup.sh define:

    * CACHE: path to directory containing these three keys
    * CACERT: path to CERN Root Certificate Authority file
    * PUBLIC_KEY: path to Grid Certification public key
    * PRIVATE_KEY: path to Grid Certificate private key
    
backup.sh is where the server will run from.

6. Follow link to set-up server-configuration: https://www.digitalocean.com/community/tutorials/how-to-serve-flask-applications-with-gunicorn-and-nginx-on-ubuntu-18-04
